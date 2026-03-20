import os, sys, re

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, LongType, FloatType, NumericType
from pyspark.sql.utils import AnalysisException
from dotenv import load_dotenv
from utils.logging import setup_logger


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
logger = setup_logger("Silver")
load_dotenv()

ADLS_ACCOUNT = os.getenv("ACCOUNT_NAME")
ADLS_CONTAINER = os.getenv("CONTAINER_NAME")
ADLS_CONNECTION_STRING = os.getenv("CONNECTION_STRING")
ADLS_ACCOUNT_KEY = os.getenv("ACCOUNT_KEY")
BRONZE_PATH = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/bronze/ibrd_batchdata.csv"
SILVER_PATH = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/silver/"

# ══════════════════════════════════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════════════════════════════════
def create_spark_session() -> SparkSession:
    """Create Spark session configured for ADLS access."""
    if not ADLS_ACCOUNT or not ADLS_ACCOUNT_KEY:
        raise ValueError("ADLS_ACCOUNT and ADLS_ACCOUNT_KEY must be set in environment variables.")
    
    spark = (
            SparkSession.builder
            .appName("IBRD_Bronze_to_Silver")
            .master("local[*]")
            .config("spark.jars", r"D:\IBRD\spark_jars\hadoop-azure-3.3.4.jar,D:\IBRD\spark_jars\azure-storage-8.6.6.jar,D:\IBRD\spark_jars\jackson-core-asl-1.9.13.jar,D:\IBRD\spark_jars\jackson-mapper-asl-1.9.13.jar") \
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.parquet.filterPushdown", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
    )
    spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    ADLS_ACCOUNT_KEY)

    spark.sparkContext.setLogLevel("WARN")
    return spark

# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORMATION FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════
def standardize_column_names(df: DataFrame) -> DataFrame:
    """Standardize column names: lowercase, snake_case, remove spaces, parentheses, and special chars."""
    logger.info("Standardizing column names to snake_case...")
    
    for col_name in df.columns:
        new_name = col_name.lower().strip()               
        new_name = new_name.replace("sum(", "sum_")       
        new_name = re.sub(r"\(.*?\)", "", new_name)       
        new_name = new_name.replace("economy", "")        
        new_name = new_name.replace("'", "")              
        new_name = new_name.replace("/", " ")             
        new_name = re.sub(r"\s+", "_", new_name)          
        new_name = re.sub(r"_+", "_", new_name)           
        new_name = new_name.strip("_")                    
        
        df = df.withColumnRenamed(col_name, new_name)
    
    logger.info(f"{len(df.columns)} columns standardized.")
    return df

def cast_numeric_columns(df):
    """
    Cast all numeric columns in the DataFrame to DoubleType.
    """
    for f in df.schema.fields:
        if isinstance(f.dataType, NumericType):
            df = df.withColumn(f.name, F.col(f.name).cast(DoubleType()))
    return df

def convert_date_columns(df: DataFrame, output_format="yyyy-MM-dd") -> DataFrame:
    """
    Safely converts date strings from inconsistent formats to a consistent date format using try_to_date.
    """
    # Columns to process
    date_cols = [
        "board_approval_date", "effective_date",
        "closed_date", "last_disbursement_date",
        "first_repayment_date", "last_repayment_date",
        "agreement_signing_date", "end_of_period"
    ]
    
    # Known input formats (add dd-MM-yyyy to handle your data)
    fmts = ["MM/dd/yyyy", "MM-dd-yyyy", "yyyy-MM-dd", "yyyy/MM/dd", "dd-MM-yyyy"]
    
    existing_cols = set(df.columns)
    
    for col in date_cols:
        if col in existing_cols:
            # Step 1: Remove unwanted characters and trim
            df = df.withColumn(col, F.regexp_replace(F.col(col), r"[^0-9/-]", ""))
            df = df.withColumn(col, F.trim(F.col(col)))
            
            # Step 2: Try multiple formats using try_to_date
            parsed_cols = [F.expr(f"try_to_date({col}, '{f}')") for f in fmts]
            df = df.withColumn(col, F.coalesce(*parsed_cols))
            
            # Step 3: format as string in consistent format
            df = df.withColumn(col, F.date_format(F.col(col), output_format))
    
    return df

def handle_missing_values(df: DataFrame) -> DataFrame:
    """Automatically fill missing values based on column data types."""

    logger.info("Handling missing values dynamically using schema...")

    numeric_cols = []
    string_cols = []

    for field in df.schema.fields:
        if isinstance(field.dataType, (DoubleType, IntegerType, LongType, FloatType)):
            numeric_cols.append(field.name)
        elif isinstance(field.dataType, StringType):
            string_cols.append(field.name)

    # Fill numeric columns with 0
    if numeric_cols:
        df = df.fillna(0.0, subset=numeric_cols)

    # Fill string columns with Unknown
    if string_cols:
        df = df.fillna("Unknown", subset=string_cols)

    return df

def handle_negative_values(df: DataFrame) -> DataFrame:
    """Set negative numeric monetary columns to 0."""
    logger.info("Handling negative values...")
    for field in df.schema.fields:
        if isinstance(field.dataType, DoubleType) and "amount" in field.name:
            df = df.withColumn(field.name,
                               F.when(F.col(field.name) < 0, 0.0)
                                .otherwise(F.col(field.name)))
    return df

def deduplicate_records(df: DataFrame) -> DataFrame:
    logger.info("Removing only exact duplicate rows...")
    df = df.dropDuplicates()
    return df

def clean_string_columns(df: DataFrame) -> DataFrame:
    """
    Trim and remove extra spaces from all string columns in the DataFrame.
    - Leading/trailing spaces removed
    - Multiple spaces replaced with a single space
    """
    string_cols = [c.name for c in df.schema.fields if str(c.dataType) == "StringType"]
    
    for col_name in string_cols:
        df = df.withColumn(col_name, F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " "))
    
    return df

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_text_columns(df: DataFrame) -> DataFrame:
    """
    Cleans specific text columns:
    - borrower: remove special characters, trim, set empty to NULL
    - project_name: remove roman numerals, numbers, special chars, normalize spaces, set empty to NULL
    """
    df = df.withColumn(
        "borrower",
        F.when(
            F.trim(F.regexp_replace(F.col("borrower"), r"[!@#\$%\^&\*\(\)\-\+,\.\?'\"]", "")) == "",
            None
        ).otherwise(
            F.trim(F.regexp_replace(F.col("borrower"), r"[!@#\$%\^&\*\(\)\-\+,\.\?'\"]", ""))
        )
    )
    df = df.withColumn(
        "project_name",
        F.when(
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col("project_name"), r"\b[IVXLCDM]+\b", ""),
                    r"[^A-Za-z ]",
                    ""
                )
            ) == "",
            None
        ).otherwise(
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col("project_name"), r"\b[IVXLCDM]+\b", ""),
                    r"[^A-Za-z ]",
                    ""
                )
            )
        )
    )

    return df

def log_data_quality(df: DataFrame) -> None:
    """Logs null percentages and distinct counts for key columns."""
    total = df.count()
    logger.info(f"DATA QUALITY — {total:,} rows, {len(df.columns)} cols")
    for c in ["region", "country", "loan_type", "loan_status"]:
        if c in df.columns:
            logger.info(f"  Distinct '{c}': {df.select(c).distinct().count()}")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
def run_bronze_to_silver(spark: SparkSession):
    """Orchestrates the full Bronze → Silver cleansing pipeline."""
    bp =  BRONZE_PATH
    sp =  SILVER_PATH

    logger.info("=" * 60)
    logger.info("BRONZE → SILVER PIPELINE STARTED")
    logger.info(f"  Input : {bp}")
    logger.info(f"  Output: {sp}")
    logger.info("=" * 60)

    try:
        # Step 1: Read raw CSV
        df = (spark.read.option("header", "true").option("inferSchema", "true")
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .csv(bp))
        logger.info(f"  Loaded {df.count():,} raw records.")

        # Quarantine corrupt records
        if "_corrupt_record" in df.columns:
            bad = df.where(F.col("_corrupt_record").isNotNull()).count()
            if bad > 0:
                logger.warning(f"  {bad:,} corrupt records quarantined.")
            df = df.where(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

        # Steps 2-6: Transform
        df = standardize_column_names(df)
        df = cast_numeric_columns(df)
        df = convert_date_columns(df)
        df = handle_missing_values(df)
        df = handle_negative_values(df)
        df = deduplicate_records(df)
        df = clean_string_columns(df)
        df = clean_text_columns(df)
        row_count = df.count()
        
        logger.info(f"Number of rows to be written to Silver: {row_count:,}")

        log_data_quality(df)

        # Step 7: Write Parquet
        logger.info("Writing Silver layer as a single Parquet file...")
        df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(SILVER_PATH)
        
        logger.info(f"✅ BRONZE → SILVER COMPLETE — {df.count():,} clean records written.")

    except AnalysisException as ae:
        logger.error(f"Spark AnalysisException: {ae}"); raise
    except Exception as exc:
        logger.critical(f"Fatal error: {exc}", exc_info=True); raise

if __name__ == "__main__":
    use_local = "--local" in sys.argv
    spark = None
    try:
        spark = create_spark_session()
        run_bronze_to_silver(spark)
    except Exception as exc:
        logger.critical(f"Pipeline failed: {exc}"); sys.exit(1)
    finally:
        if spark: spark.stop()
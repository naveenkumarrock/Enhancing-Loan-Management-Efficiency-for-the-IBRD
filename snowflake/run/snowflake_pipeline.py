import os
import snowflake.connector
from dotenv import load_dotenv
from utils.logging import setup_logger

logger = setup_logger("Snowflake")
load_dotenv()

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}


def run_sql_file(sql_file_path):

    logger.info("Connecting to Snowflake...")

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"],
    )

    try:

        logger.info(f"Reading SQL file: {sql_file_path}")

        with open(sql_file_path, "r") as f:
            sql_script = f.read()

        # Execute all SQL commands
        for cursor in conn.execute_string(sql_script):
            print("Executed:", cursor)

        logger.info("SQL execution completed successfully.")

    except Exception as e:
        logger.error("Error executing SQL:", e)

    finally:
        conn.close()
        logger.info("Snowflake connection closed.")


if __name__ == "__main__":

    sql_files = ["snowflake/setup/datawarehouse.sql","snowflake/setup/datamasking_rbac.sql"]

    for file in sql_files:
        run_sql_file(file)
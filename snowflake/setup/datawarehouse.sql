-- ==========================================================
-- 1. CREATE WAREHOUSE
-- ==========================================================

CREATE WAREHOUSE IF NOT EXISTS LOAN_WH
WITH
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE;

USE WAREHOUSE LOAN_WH;


-- ==========================================================
-- 2. CREATE DATABASE
-- ==========================================================

CREATE DATABASE IF NOT EXISTS LOAN_DB;

USE DATABASE LOAN_DB;


-- ==========================================================
-- 3. USE SCHEMA
-- ==========================================================

USE SCHEMA PUBLIC;


-- ==========================================================
-- 4. DIMENSION TABLES
-- ==========================================================

CREATE TABLE IF NOT EXISTS DIM_COUNTRY (
    COUNTRY_KEY BIGINT,
    COUNTRY_CODE STRING,
    COUNTRY STRING,
    REGION STRING,
    GUARANTOR_COUNTRY_CODE STRING,
    GUARANTOR STRING
);

CREATE OR REPLACE TABLE DIM_PROJECT (
    PROJECT_KEY BIGINT,
    PROJECT_ID STRING,
    PROJECT_NAME STRING
);

CREATE TABLE IF NOT EXISTS DIM_LOAN_TYPE (
    LOAN_TYPE_KEY BIGINT,
    LOAN_TYPE STRING,
    LOAN_STATUS STRING
);

CREATE OR REPLACE TABLE DIM_BORROWER (
    BORROWER_KEY BIGINT,
    BORROWER STRING,
    COUNTRY_CODE STRING
);


-- ==========================================================
-- 5. FACT TABLE
-- ==========================================================

CREATE OR REPLACE TABLE FACT_LOANS (

    LOAN_FACT_KEY BIGINT,

    COUNTRY_KEY BIGINT,
    PROJECT_KEY BIGINT,
    LOAN_TYPE_KEY BIGINT,
    BORROWER_KEY BIGINT,

    LOAN_NUMBER STRING,
    END_OF_PERIOD DATE,

    BOARD_APPROVAL_DATE DATE,
    AGREEMENT_SIGNING_DATE DATE,
    FIRST_REPAYMENT_DATE DATE,
    LAST_REPAYMENT_DATE DATE,
    LAST_DISBURSEMENT_DATE DATE,

    ORIGINAL_PRINCIPAL_AMOUNT FLOAT,
    CANCELLED_AMOUNT FLOAT,
    UNDISBURSED_AMOUNT FLOAT,
    DISBURSED_AMOUNT FLOAT,

    REPAID_TO_IBRD FLOAT,
    DUE_TO_IBRD FLOAT,

    BORROWERS_OBLIGATION FLOAT,

    SOLD_3RD_PARTY FLOAT,
    REPAID_3RD_PARTY FLOAT,
    DUE_3RD_PARTY FLOAT,

    LOANS_HELD FLOAT,

    INTEREST_RATE FLOAT
);


-- ==========================================================
-- 6. CREATE EXTERNAL STAGE (ADLS GOLD LAYER)
-- ==========================================================

CREATE OR REPLACE STAGE GOLD_STAGE
URL='azure://loanefficiency.blob.core.windows.net/naveen/gold/'
CREDENTIALS=(
AZURE_SAS_TOKEN='sp=rcwl&st=2026-03-15T10:50:35Z&se=2026-03-31T19:05:35Z&spr=https&sv=2024-11-04&sr=c&sig=n1YZgVfbtW8JbAUc30RmeJPQO4NwgbiDSFPdtXaKdm0%3D'
)
FILE_FORMAT=(TYPE=PARQUET);

-- ==========================================================
-- 7. LOAD DATA INTO DIMENSIONS
-- ==========================================================

LIST @GOLD_STAGE;

COPY INTO DIM_COUNTRY
FROM (
SELECT
    $1:COUNTRY_KEY::BIGINT,
    NULLIF($1:COUNTRY_CODE::STRING,'Unknown'),
    NULLIF($1:COUNTRY::STRING,'Unknown'),
    NULLIF($1:REGION::STRING,'Unknown'),
    NULLIF($1:GUARANTOR_COUNTRY_CODE::STRING,'Unknown'),
    NULLIF($1:GUARANTOR::STRING,'Unknown')
FROM @GOLD_STAGE/dim_country/
)
FILE_FORMAT=(TYPE=PARQUET)
PATTERN='.*\\.parquet'
ON_ERROR='CONTINUE';

COPY INTO DIM_PROJECT
FROM (
SELECT
    $1:project_key::BIGINT,
    NULLIF($1:project_id::STRING,'Unknown'),

    TRIM(REGEXP_REPLACE(REGEXP_REPLACE($1:project_name::STRING,'\\b[IVXLCDM]+\\b',''),'[^A-Za-z0-9 ]',''))

FROM @GOLD_STAGE/dim_project/
)
FILE_FORMAT=(TYPE=PARQUET)
PATTERN='.*\\.parquet'
ON_ERROR='CONTINUE';

COPY INTO DIM_LOAN_TYPE
FROM (
SELECT
    $1:LOAN_TYPE_KEY::BIGINT,
    NULLIF($1:LOAN_TYPE::STRING,'Unknown'),
    NULLIF($1:LOAN_STATUS::STRING,'Unknown')
FROM @GOLD_STAGE/dim_loan_type/
)
FILE_FORMAT=(TYPE=PARQUET)
PATTERN='.*\\.parquet'
ON_ERROR='CONTINUE';

COPY INTO DIM_BORROWER
FROM (
SELECT
    $1:borrower_key::BIGINT,
    NULLIF(REGEXP_REPLACE($1:borrower::STRING, '[!@#\$%\^&\*\(\)\-\+,.?''"]', ''), '') AS borrower,
    NULLIF($1:country_code::STRING,'Unknown') AS country_code
FROM @GOLD_STAGE/dim_borrower/
)
FILE_FORMAT=(TYPE=PARQUET)
PATTERN='.*\\.parquet'
ON_ERROR='CONTINUE';

-- ==========================================================
-- 8. LOAD FACT TABLE
-- ==========================================================
COPY INTO FACT_LOANS
FROM (
    SELECT
        $1:loan_fact_key::BIGINT,
        $1:country_key::BIGINT,
        $1:project_key::BIGINT,
        $1:loan_type_key::BIGINT,
        $1:borrower_key::BIGINT,
        $1:loan_number::STRING,
        TRY_TO_DATE($1:end_of_period::STRING) AS end_of_period,
        TRY_TO_DATE($1:board_approval_date::STRING) AS board_approval_date,
        COALESCE(TRY_TO_DATE($1:agreement_signing_date::STRING),'2020-01-01') AS agreement_signing_date,
        TRY_TO_DATE($1:first_repayment_date::STRING) AS first_repayment_date,
        TRY_TO_DATE($1:last_repayment_date::STRING) AS last_repayment_date,
        COALESCE(TRY_TO_DATE($1:last_disbursement_date::STRING), '2020-01-01') AS last_disbursement_date,
        COALESCE($1:original_principal_amount::FLOAT, 0.0) AS original_principal_amount,
        COALESCE($1:cancelled_amount::FLOAT, 0.0) AS cancelled_amount,
        COALESCE($1:undisbursed_amount::FLOAT, 0.0) AS undisbursed_amount,
        COALESCE($1:disbursed_amount::FLOAT, 0.0) AS disbursed_amount,
        COALESCE($1:repaid_to_ibrd::FLOAT, 0.0) AS repaid_to_ibrd,
        COALESCE($1:due_to_ibrd::FLOAT, 0.0) AS due_to_ibrd,
        COALESCE($1:borrowers_obligation::FLOAT, 0.0) AS borrowers_obligation,
        COALESCE($1:sold_3rd_party::FLOAT, 0.0) AS sold_3rd_party,
        COALESCE($1:repaid_3rd_party::FLOAT, 0.0) AS repaid_3rd_party,
        COALESCE($1:due_3rd_party::FLOAT, 0.0) AS due_3rd_party,
        COALESCE($1:loans_held::FLOAT, 0.0) AS loans_held,
        COALESCE($1:interest_rate::FLOAT, 0.0) AS interest_rate
    FROM @GOLD_STAGE/fact_loans/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR='CONTINUE';


-- ==========================================================
-- 9. VALIDATION QUERIES
-- ==========================================================

SELECT COUNT(*) FROM DIM_COUNTRY;

SELECT COUNT(*) FROM DIM_PROJECT;

SELECT COUNT(*) FROM DIM_LOAN_TYPE;

SELECT COUNT(*) FROM DIM_BORROWER;

SELECT COUNT(*) FROM FACT_LOANS;
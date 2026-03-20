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

CREATE OR REPLACE TABLE DIM_COUNTRY (
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

CREATE OR REPLACE TABLE DIM_LOAN_TYPE (
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
URL = 'azure://loanefficiency.blob.core.windows.net/naveen/gold/'
STORAGE_INTEGRATION = AZURE_INT
FILE_FORMAT = (TYPE = PARQUET);

CREATE OR REPLACE FILE FORMAT my_parquet_format
TYPE = PARQUET;

CREATE OR REPLACE NOTIFICATION INTEGRATION AZURE_NOTIFICATION
TYPE = QUEUE
ENABLED = TRUE
NOTIFICATION_PROVIDER = 'AZURE_STORAGE_QUEUE'
AZURE_TENANT_ID = '9cefb894-54ee-4e0d-91e7-908ce3342271'
AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://loanefficiency.queue.core.windows.net/snowpipequeues';

describe integration AZURE_NOTIFICATION;
-- ==========================================================
-- 8. LOAD DATA INTO DIM AND FACT TABLES
-- ==========================================================
CREATE OR REPLACE PIPE PIPE_DIM_COUNTRY
AUTO_INGEST = TRUE
INTEGRATION = AZURE_NOTIFICATION
AS
COPY INTO DIM_COUNTRY
FROM (
    SELECT
        $1:country_key::NUMBER AS COUNTRY_KEY,
        UPPER($1:country_code::STRING) AS COUNTRY_CODE,
        INITCAP($1:country::STRING) AS COUNTRY,
        UPPER($1:region::STRING) AS REGION,
        UPPER($1:guarantor_country_code::STRING) AS GUARANTOR_COUNTRY_CODE,
        INITCAP($1:guarantor::STRING) AS GUARANTOR
    FROM @GOLD_STAGE/dim_country/
);

CREATE OR REPLACE PIPE PIPE_DIM_PROJECT
AUTO_INGEST = TRUE
INTEGRATION = AZURE_NOTIFICATION
AS
COPY INTO DIM_PROJECT
FROM (
    SELECT
        $1:project_key,
        $1:project_id,
        $1:project_name
    FROM @GOLD_STAGE/dim_project/
);

CREATE OR REPLACE PIPE PIPE_DIM_LOAN_TYPE
AUTO_INGEST = TRUE
INTEGRATION = AZURE_NOTIFICATION
AS
COPY INTO DIM_LOAN_TYPE
FROM (
    SELECT
        $1:loan_type_key::NUMBER AS LOAN_TYPE_KEY,
        UPPER($1:loan_type::STRING) AS LOAN_TYPE,
        INITCAP($1:loan_status::STRING) AS LOAN_STATUS
    FROM @GOLD_STAGE/dim_loan_type/
);

CREATE OR REPLACE PIPE PIPE_DIM_BORROWER
AUTO_INGEST = TRUE
INTEGRATION = AZURE_NOTIFICATION
AS
COPY INTO DIM_BORROWER
FROM (
    SELECT
        $1:borrower_key,
        $1:borrower,
        $1:country_code
    FROM @GOLD_STAGE/dim_borrower/
);

CREATE OR REPLACE PIPE PIPE_FACT_LOANS
AUTO_INGEST = TRUE
INTEGRATION = AZURE_NOTIFICATION
AS
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
-- 9. GRANTS FOR ROLE
-- ==========================================================
GRANT USAGE ON STAGE GOLD_STAGE TO ROLE ACCOUNTADMIN;
GRANT OPERATE ON PIPE PIPE_DIM_COUNTRY TO ROLE ACCOUNTADMIN;
GRANT OPERATE ON PIPE PIPE_DIM_PROJECT TO ROLE ACCOUNTADMIN;
GRANT OPERATE ON PIPE PIPE_DIM_LOAN_TYPE TO ROLE ACCOUNTADMIN;
GRANT OPERATE ON PIPE PIPE_DIM_BORROWER TO ROLE ACCOUNTADMIN;
GRANT OPERATE ON PIPE PIPE_FACT_LOANS TO ROLE ACCOUNTADMIN;
DESC TABLE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER;

DESC TABLE SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS;

USE WAREHOUSE COMPUTE_WH;

USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE OR REPLACE TABLE SILVER.CUSTOMER_DIM(
    CUSTOMER_SK NUMBER AUTOINCREMENT,
    CUSTOMER_ID NUMBER,
    STATE STRING,
    START_DATE DATE,
    END_DATE DATE,
    IS_CURRENT STRING
);

INSERT INTO SILVER.CUSTOMER_DIM (CUSTOMER_ID, STATE, START_DATE, END_DATE, IS_CURRENT)
SELECT
    C.C_CUSTOMER_SK,
    CA.CA_STATE,
    CURRENT_DATE,
    NULL,
    'Y'
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER C
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS CA
    ON C.C_CURRENT_ADDR_SK = CA.CA_ADDRESS_SK;

SELECT COUNT(*) FROM SILVER.CUSTOMER_DIM;

SELECT * FROM SILVER.CUSTOMER_DIM LIMIT 10;

CREATE OR REPLACE TABLE SILVER.CUSTOMER_STAGE AS
SELECT
    CUSTOMER_ID,STATE
FROM SILVER.CUSTOMER_DIM
WHERE IS_CURRENT = 'Y';

update silver.customer_stage
set state='TX'
where customer_id=83369285;

select * from silver.customer_stage where customer_id=83369285;

--implementing stage 2 using merge

merge into silver.customer_dim target
using silver.customer_stage source
on target.customer_id=source.customer_id and target.is_current='Y'

when matched and target.state <> source.state then
update set 
    target.end_date=current_date,
    target.is_current='N'

when not matched then
insert (customer_id, state, start_date, end_date, is_current)
values (source.customer_id, source.state, current_date, NULL, 'Y');

select * from silver.customer_dim where customer_id=83369285 ;
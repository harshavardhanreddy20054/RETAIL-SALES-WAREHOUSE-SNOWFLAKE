
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE SCHEMA IF NOT EXISTS GOLD;


CREATE OR REPLACE TABLE GOLD.SALES_BY_STATE AS
SELECT 
    d.state,
    SUM(f.sales_price) AS total_sales
FROM SILVER.FACT_SALES f
JOIN SILVER.CUSTOMER_DIM d
    ON f.customer_sk = d.customer_sk
GROUP BY d.state;

CREATE OR REPLACE TABLE GOLD.SALES_BY_YEAR AS
SELECT 
    YEAR(f.transaction_date) AS sales_year,
    SUM(f.sales_price) AS total_sales
FROM SILVER.FACT_SALES f
GROUP BY SALES_YEAR;

create or replace table GOLD.monthly_sales AS
select
    DATE_TRUNC('month', f.transaction_date) AS sales_month,
    SUM(f.sales_price) AS total_sales
from SILVER.FACT_SALES f
group by sales_month;

create or replace table GOLD.top_stores AS
select
    store_id,
    SUM(f.sales_price) AS total_sales
from SILVER.FACT_SALES f
group by store_id
order by total_sales desc
limit 10;

--validation queries
select count(*) from GOLD.SALES_BY_STATE;
select count(*) from GOLD.SALES_BY_YEAR;
select count(*) from GOLD.monthly_sales;

select * from GOLD.monthly_sales;
select count(*) from GOLD.top_stores;

select sum(sales_price) from silver.fact_sales;

select sum(total_sales) from GOLD.sales_by_state;

select sum(total_sales) from GOLD.sales_by_year;

select sum(total_sales) from GOLD.monthly_sales;

select sum(total_sales) from GOLD.top_stores;
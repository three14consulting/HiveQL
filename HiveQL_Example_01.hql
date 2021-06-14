-------------------------------------------------------------------------------
-- Databases, tables, columns, and values have been renamed and revalued
-- for data protection, the account numbers in the final result have been 
-- scrambled and masked.
-------------------------------------------------------------------------------
-- OBJECTIVE:
--   Query that finds the top 100 Premium accounts by 12 month moving average
--   of last 12 months of spend compared to the previous 12 months of spend.
--
-- FEATURES USED:
--   * Hive parameters
--   * SQL Statements: DROP TABLE,
--                     CREATE TABLE,
--                     SELECT,
--                     INSERT INTO
--   * Sub-queries
--   * Joins
--   * Windowing functions: LEAD(), AVG()
--   * Analytical functions: RANK()
--   * Hive functions: HASH(), ABS(), CAST(), MASK_SHOW_LAST_N()
-------------------------------------------------------------------------------

-- Hive Parameters
SET hivevar:end_dt = date_format(add_months(CURRENT_DATE(),-1), 'YYYY-MM');
SET hivevar:start_dt = date_format(add_months(CURRENT_DATE(),-36),'YYYY-MM');
SET hivevar:chk_dt = date_format(add_months(CURRENT_DATE(),-13), 'YYYY-MM');

-- Use temporary database for interim results, tables of which are dropped
-- after successful execution of entire query
USE tmpdb;

-- Create table to store final results
DROP TABLE IF EXISTS prsnldb.top100_premium_accounts;
CREATE TABLE prsnldb.top100_premium_accounts
             (
                          account_no CHAR(10)
                        , account_rank       INT
             ) ;

-- Initial data pull
DROP TABLE IF EXISTS premium_financials;
CREATE TEMPORARY TABLE premium_financials AS 
SELECT 
    d.*
FROM   (
          SELECT 
                  a.account_no
                , a.calendar_dt
                , a.account_spend
                , COUNT(a.calendar_dt) OVER (PARTITION BY a.account_no) 
                  AS months
          FROM   scnddb.financials_tbl AS a
          WHERE  account_no IN
                          (
                          SELECT DISTINCT 
                                          b.account_no
                          FROM            maindb.account_demographics AS b
                          INNER JOIN      scnddb.product_tbl          AS c
                          ON              b.crd_parent = c.crd_product
                          AND             b.crd_child = c.crd_code
                          WHERE           c.acc_org = '999'
                          AND             b.acc_org = '999'
                          AND             c.card_family = 'PREMIUM' )
          AND    a.calendar_dt <= ${end_dt}
          AND    a.calendar_dt >= ${start_dt} ) AS d
WHERE  d.months = 36 ;

-- Add most recent rolling 12 months total and the rolling
-- 12 months total, 12 months prior, and the percentage change
DROP TABLE IF EXISTS premium_with_rolling;
CREATE TEMPORARY TABLE premium_with_rolling AS 
SELECT 
       b.account_no
     , b.calendar_dt
     , b.r12_cy_spend
     , b.r12_py_spend
     , (b.r12_cy_spend/b.r12_py_spend)-1 AS r12_change
FROM   (
        SELECT   
                 a.*
               , lead(a.r12_cy_spend, 12) OVER (PARTITION BY account_no 
                                                ORDER BY calendar_dt DESC)
                                                AS r12_py_spend
        FROM     (
                  SELECT   
                           account_no
                         , calendar_dt
                         , account_spend
                         , sum(account_spend) OVER (PARTITION BY account_no 
                                                     ORDER BY calendar_dt DESC 
                                                     ROWS BETWEEN CURRENT ROW 
                                                     AND 11 FOLLOWING) 
                                                     AS r12_cy_spend
                  FROM     premium_financials
                  ORDER BY 
                            account_no
                          , calendar_dt DESC 
                  ) AS a 
        ) AS b
    --Filter out rows with invalid r12_cy_spend & r12_py_spend
WHERE  b.calendar_dt >= ${chk_dt} ;

-- Take the rolling average of the change and rank the accounts
DROP TABLE IF EXISTS premium_accounts_ranked;
CREATE TEMPORARY TABLE premium_accounts_ranked AS
SELECT   
         a.account_no
       , a.r12_change_average
       , RANK() OVER (ORDER BY a.r12_change_average DESC) AS account_rank
FROM     (
          SELECT   
                 account_no
                 , AVG(r12_change) AS r12_change_average
          FROM     premium_with_rolling
          GROUP BY 1 
          ) AS a ;

-- Take top 100
DROP TABLE IF EXISTS top100_premium_accounts;
CREATE TEMPORARY TABLE top100_premium_accounts AS
SELECT    
         MASK_SHOW_LAST_N(ABS(HASH(account_no)), 4)
                 AS account_no -- SCRAMBLED & MASKED
       , account_rank
FROM     premium_accounts_ranked
WHERE    account_rank <= 100
ORDER BY account_rank ;

-- QC
SELECT 
    * 
FROM   top100_premium_accounts;

-- Finally, insert into results table
INSERT INTO table prsnldb.top100_premium_accounts
SELECT 
    *
FROM   top100_premium_accounts;

-- Print top 10
SELECT * FROM  prsnldb.top100_premium_accounts LIMIT 10;

--    account_no    account_rank
--    XXXXXX4369    1
--    XXXXXX8064    2
--    XXXXXX8607    3
--    XXXXXX3412    4
--    XXXXXX6716    5
--    XXXXXX8320    6
--    XXXXXX0270    7
--    XXXXXX7704    8
--    XXXXXX4583    9
--    XXXXXX2284    10
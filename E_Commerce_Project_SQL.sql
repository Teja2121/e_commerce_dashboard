USE e_commerce;

SELECT *
FROM amz_sl_rp;

/* Here Date is not of the Date format so we need to modify that */
ALTER TABLE amz_sl_rp 
ADD COLUMN Date_temp DATE;

UPDATE amz_sl_rp
SET Date_temp = STR_TO_DATE(Date, '%Y-%m-%d');

SELECT Date, Date_temp FROM amz_sl_rp 
LIMIT 10;

ALTER TABLE amz_sl_rp 
DROP COLUMN Date;

ALTER TABLE amz_sl_rp 
CHANGE Date_temp Date DATE;

/* ship-postal-code should also be change to text instead of double */
ALTER TABLE amz_sl_rp
MODIFY COLUMN `ship-postal-code` VARCHAR(20);

/* Checking all the columns for distinct values that can later be used to make indexes off the distinct characters */
SELECT DISTINCT Category
FROM amz_sl_rp;

/* Some values in state and city columns need to standardized and cleaned */
SELECT DISTINCT `ship-state`
FROM amz_sl_rp;

/* There are some states in short-form, let's convert them into their full form */
SELECT DISTINCT `ship-state`
FROM amz_sl_rp
ORDER BY `ship-state` ASC;

SELECT DISTINCT `ship-state`
FROM amz_sl_rp
WHERE LENGTH(`ship-state`) < 3;

UPDATE amz_sl_rp
SET `ship-state` = CASE `ship-state`
	WHEN 'NL' THEN 'NAGALAND'
    WHEN 'RJ' THEN 'RAJASTHAN'
    WHEN 'PB' THEN 'PUNJAB'
    WHEN 'AR' THEN 'ARUNACHAL PRADESH'
    ELSE `ship-state`
END;

/* Some states also names also need to changed because of spelling errors */
UPDATE amz_sl_rp
SET `ship-state` = CASE `ship-state`
	WHEN 'RAJSHTHAN' THEN 'RAJASTHAN'
    WHEN 'RAJSTHAN' THEN 'RAJASTHAN'
    WHEN 'ODISHA' THEN 'ORISSA'
    WHEN 'PUDUCHERRY' THEN 'PONDICHERRY'
    WHEN 'PUNJAB/MOHALI/ZIRAKPUR' THEN 'PUNJAB'
    ELSE `ship-state`
END;

CREATE INDEX idx_date
ON amz_sl_rp(`Date`);

CREATE INDEX idx_category 
ON amz_sl_rp(`Category`(2));

UPDATE amz_sl_rp
SET `ship-state` = UPPER(`ship-state`);

CREATE INDEX idx_state 
ON amz_sl_rp(`ship-state`(3));

CREATE INDEX idx_country 
ON amz_sl_rp(`ship-country`(1));

SELECT DISTINCT `ship-city`
FROM amz_sl_rp
ORDER BY `ship-city` ASC;

/* Let's also standardize the city */ 
UPDATE amz_sl_rp
SET `ship-city` = UPPER(`ship-city`);

/* There are some misprints in cities too, lets update them */
SELECT `ship-city`, REGEXP_REPLACE(`ship-city`, '[^a-zA-Z\\s]', '') AS cleaned_set
FROM amz_sl_rp
ORDER BY `ship-city` ASC;

UPDATE amz_sl_rp
SET `ship-city` = REGEXP_REPLACE(`ship-city`, '[^a-zA-Z\\s]', '') ;

SELECT DISTINCT `ship-city`
FROM amz_sl_rp
WHERE LENGTH(`ship-city`) > 20
ORDER BY `ship-city` ASC;

SELECT *
FROM amz_sl_rp
WHERE `ship-city` = '';

DELETE FROM amz_sl_rp
WHERE `ship-city` IS NULL OR `ship-city` = '';

SELECT MAX(LENGTH(`promotion-ids`))
FROM amz_sl_rp;

/* Partioning by months */
DROP TABLE IF EXISTS amz_sl_rp_partitioned;

CREATE TABLE amz_sl_rp_partitioned (
    `Order ID` VARCHAR(50),
    `Status` VARCHAR(50),
    `Fulfilment` VARCHAR(50),
    `Sales Channel` VARCHAR(50),
    `ship-service-level` VARCHAR(50),
    `Style` VARCHAR(100),
    `SKU` VARCHAR(100),
    `Category` VARCHAR(100),
    `Size` VARCHAR(20),
    `ASIN` VARCHAR(50),
    `Courier Status` VARCHAR(50),
    `Qty` INT,
    `currency` VARCHAR(10),
    `Amount` FLOAT,
    `ship-city` VARCHAR(100),
    `ship-state` VARCHAR(100),
    `ship-postal-code` VARCHAR(20),
    `ship-country` VARCHAR(50),
    `promotion-ids` VARCHAR(2400),
    `B2B` VARCHAR(50),
    `fulfilled-by` VARCHAR(50),
    `Year` INT,
    `Month` INT,
	`Date` DATE
)
PARTITION BY RANGE (`Month`) (
    PARTITION p01 VALUES LESS THAN (2),
    PARTITION p02 VALUES LESS THAN (3),
    PARTITION p03 VALUES LESS THAN (4),
    PARTITION p04 VALUES LESS THAN (5),
    PARTITION p05 VALUES LESS THAN (6),
    PARTITION p06 VALUES LESS THAN (7),
    PARTITION p07 VALUES LESS THAN (8),
    PARTITION p08 VALUES LESS THAN (9),
    PARTITION p09 VALUES LESS THAN (10),
    PARTITION p10 VALUES LESS THAN (11),
    PARTITION p11 VALUES LESS THAN (12),
    PARTITION p12 VALUES LESS THAN (13)
);

INSERT INTO amz_sl_rp_partitioned
SELECT * 
FROM amz_sl_rp;


SELECT 
    partition_name, 
    table_rows 
FROM information_schema.partitions 
WHERE table_name = 'amz_sl_rp_partitioned';

/* Now let's check if partition is working */
EXPLAIN 
SELECT *
FROM amz_sl_rp_partitioned
WHERE `Month` = 3;

/* Sales per month*/
SELECT `Month`, SUM(Amount) AS Total_Sales
FROM amz_sl_rp_partitioned
GROUP BY `Month`
ORDER BY `Month`;

/* Stored Procedure for monthly orders and monthly sales*/
DELIMITER //
CREATE PROCEDURE GetMonthlySalesSummary()
BEGIN
	SELECT `Month`,
    SUM(Amount) AS Total_sales,
    COUNT(*) AS Total_Orders
    FROM amz_sl_rp_partitioned
    GROUP BY `Month`
    ORDER BY `Month`;
END //

DELIMITER ;

CALL GetMonthlySalesSummary();

/* Find the number 1 category in each month */
SELECT Category, Month, Total_Sales
FROM (
    SELECT 
        Category,
        Month,
        ROUND(SUM(Amount),2) AS Total_Sales,
        RANK() OVER (PARTITION BY Month ORDER BY SUM(Amount) DESC) AS sales_rank
    FROM amz_sl_rp_partitioned
    GROUP BY Month, Category
) ranked
WHERE sales_rank = 1;

/* Percentage change in sales by each month*/
SELECT 
    `Month`,
    SUM(Amount) AS Total_Sales,
    LAG(SUM(Amount)) OVER (ORDER BY `Month`) AS Previous_Month_Sales,
    ROUND(((SUM(Amount) - LAG(SUM(Amount)) OVER (ORDER BY `Month`)) / 
           LAG(SUM(Amount)) OVER (ORDER BY `Month`)) * 100, 2) AS Growth_Rate
FROM amz_sl_rp_partitioned
GROUP BY `Month`;

CREATE VIEW vw_monthly_category_sales AS
SELECT 
    `Month`,
    `Category`,
    SUM(Amount) AS Total_Sales,
    COUNT(*) AS Total_Orders
FROM amz_sl_rp_partitioned
GROUP BY `Month`, `Category`
ORDER BY `Month`, Total_Sales DESC;

/* Average Order Value per Month*/
SELECT 
    `Month`,
    ROUND(SUM(Amount) / COUNT(DISTINCT `Order ID`), 2) AS Average_Order_Value
FROM amz_sl_rp_partitioned
GROUP BY `Month`
ORDER BY `Month`;





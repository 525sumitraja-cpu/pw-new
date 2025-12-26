CREATE TABLE customers (
    Customer_ID INT,
    Name VARCHAR(50),
    City VARCHAR(50),
    Monthly_Sales INT,
    Income INT,
    Region VARCHAR(20)
);

INSERT INTO customers VALUES
(101, 'Rahul Mehta', 'Mumbai', 12000, 65000, 'West'),
(102, 'Anjali Rao', 'Bengaluru', NULL, NULL, 'South'),
(103, 'Suresh Iyer', 'Chennai', 15000, 72000, 'South'),
(104, 'Neha Singh', 'Delhi', NULL, NULL, 'North'),
(105, 'Amit Verma', 'Pune', 18000, 58000, NULL),
(106, 'Karan Shah', 'Ahmedabad', NULL, 61000, 'West'),
(107, 'Pooja Das', 'Kolkata', 14000, NULL, 'East'),
(108, 'Riya Kapoor', 'Jaipur', 16000, 69000, 'North');
select * from customers;
-- Q1. What are the most common reasons for missing data in ETL pipelines?
SELECT *
FROM customers
WHERE Monthly_Sales IS NULL
   OR Income IS NULL
   OR Region IS NULL;
   
  -- Q2. Why is blindly deleting rows with missing values considered a bad practice in ETL?
  
  SELECT
    SUM(CASE WHEN Monthly_Sales IS NULL THEN 1 ELSE 0 END) AS Missing_Monthly_Sales,
    SUM(CASE WHEN Income IS NULL THEN 1 ELSE 0 END) AS Missing_Income,
    SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END) AS Missing_Region
FROM customers;

-- Q3. Explain the difference between Listwise deletion ,Column deletion Also mention one scenario where each is appropri

SELECT *,
CASE
    WHEN Income IS NULL THEN 1
    ELSE 0
END AS Income_Missing_Flag
FROM customers;

-- Q4. Why is median imputation preferred over mean imputation for skewed data such as income?
SELECT AVG(Income) AS Median_Income
FROM (
    SELECT Income
    FROM customers
    WHERE Income IS NOT NULL
    ORDER BY Income
    LIMIT 2 OFFSET 2
) t;

-- Q5. What is forward fill and in what type of dataset is it most useful?

SET SQL_SAFE_UPDATES = 0;
UPDATE customers
SET Income = (
    SELECT AVG(Income)
    FROM (
        SELECT Income
        FROM customers
        WHERE Income IS NOT NULL
        ORDER BY Income
        LIMIT 2 OFFSET 2
    ) t
)
WHERE Income IS NULL;


  -- Q6. Why should flagging missing values be done before imputation in an ETL workflow?
SET SQL_SAFE_UPDATES = 0;

UPDATE customers
SET Monthly_Sales = (
    SELECT AVG(Monthly_Sales)
    FROM customers
    WHERE Monthly_Sales IS NOT NULL
)
WHERE Monthly_Sales IS NULL;


-- Q7. Consider a scenario where income is missing for many customers.How can this missingness itself 
UPDATE customers 
SET 
    Region = 'Unknown'
WHERE
    Region IS NULL;
SET SQL_SAFE_UPDATES = 1;

-- Q8. Listwise Deletion Remove all rows where Region is missing.
 -- Tasks:Identify affected rows ,Show the dataset after deletion ,Mention how many records were lost
 
 SELECT *
FROM customers
WHERE Region IS NULL;

SELECT COUNT(*) AS Records_Lost
FROM customers
WHERE Region IS NULL;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM customers
WHERE Region IS NULL;

SELECT * FROM customers;

-- Q9. Imputation Handle missing values in Monthly_Sales using: Forward Fill
-- task Apply forward fill, Show before vs after values ,Explain why forward fill is suitable her
SELECT Customer_ID, Monthly_Sales
FROM customers
ORDER BY Customer_ID;

 UPDATE customers c1
JOIN (
    SELECT Customer_ID,
           MAX(Monthly_Sales) OVER (
               ORDER BY Customer_ID
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS Filled_Sales
    FROM customers
) c2
ON c1.Customer_ID = c2.Customer_ID
SET c1.Monthly_Sales = c2.Filled_Sales
WHERE c1.Monthly_Sales IS NULL;

SELECT Customer_ID, Monthly_Sales
FROM customers
ORDER BY Customer_ID;

-- Q10. Flagging Missing Data Create a flag column for missing Income.
-- Tasks:Create Income_Missing_Flag (0 = present, 1 = missing),Show updated dataset ,ount how many customers have missing income


SET SQL_SAFE_UPDATES = 0;

UPDATE customers
SET Income_Missing_Flag =
CASE
    WHEN Income IS NULL THEN 1
    ELSE 0
END;
SELECT *
FROM customers;
SELECT COUNT(*) AS Missing_Income_Customers
FROM customers
WHERE Income_Missing_Flag = 1;
SET SQL_SAFE_UPDATES = 1;


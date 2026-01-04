CREATE DATABASE Retail_Database
 
USE Retail_Database

CREATE TABLE retail_data (
    TransactionID INT PRIMARY KEY,
    TransactionDate DATE,
    CustomerID VARCHAR(50),
    Gender VARCHAR(10),
    Age INT,
    ProductCategory VARCHAR(50),
    Quantity INT,
    PricePerUnit DECIMAL(10,2),
    TotalAmount DECIMAL(12,2)
)

select * from retail_data


-- To load raw data in sql server
BULK INSERT retail_data
FROM 'C:\Users\allad\Desktop\SQL\Cloud_Retail_Pipeline\data\retail_sales_dataset.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK,
    CODEPAGE = '65001'   -- UTF-8 support
);

select * from retail_data

-- Applying all aggregation functions with 'group by' clause
SELECT COUNT(*) FROM retail_data

SELECT ProductCategory, SUM(TotalAmount) AS TotalSales FROM retail_data GROUP BY ProductCategory

SELECT Gender, AVG(TotalAmount) AS AvgSpend FROM retail_data GROUP BY Gender

SELECT TransactionDate, SUM(TotalAmount) AS DailySales FROM retail_data GROUP BY TransactionDate ORDER BY TransactionDate;


--Monthly revenue

SELECT 
    DATEADD(MONTH, DATEDIFF(MONTH, 0, TransactionDate), 0) AS [Month],
    SUM(TotalAmount) AS Revenue
FROM retail_data
GROUP BY 
    DATEADD(MONTH, DATEDIFF(MONTH, 0, TransactionDate), 0)
ORDER BY [Month];


-- TO DELETE TABLE IN DATABASE 

USE Retail_Database

drop table retail_data

--



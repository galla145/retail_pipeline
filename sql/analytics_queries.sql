-- Total sales by category
SELECT ProductCategory, SUM(TotalAmount) AS TotalSales
FROM retail_data
GROUP BY ProductCategory;


-- Monthly revenue
SELECT DATE_TRUNC('month', TransactionDate) AS Month, SUM(TotalAmount) AS Revenue
FROM retail_data
GROUP BY Month;

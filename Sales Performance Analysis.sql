-- Databricks notebook source
CREATE TABLE SalesPerformance (
    SalesID INT ,
    EmpName VARCHAR(100),
    Region VARCHAR(50),
    Month DATE,
    SalesAmount DECIMAL(10, 2),
    Salary DECIMAL(10,2)
);

-- COMMAND ----------

INSERT INTO SalesPerformance (SalesID, EmpName, Region, Month, SalesAmount, Salary) VALUES
(1, 'Alice', 'North', '2025-01-01', 5000.00, 4000.00),
(2, 'Bob', 'South', '2025-01-01', 7000.00, 4200.00),
(3, 'Charlie', 'East', '2025-01-01', 5500.00, 4300.00),
(4, 'David', 'West', '2025-01-01', 4800.00, 4100.00),
(5, 'Eve', 'North', '2025-01-01', 5300.00, 4050.00),
(6, 'Alice', 'North', '2025-02-01', 6000.00, 4000.00),
(7, 'Bob', 'South', '2025-02-01', 7200.00, 4200.00),
(8, 'Charlie', 'East', '2025-02-01', 5800.00, 4300.00),
(9, 'David', 'West', '2025-02-01', 4900.00, 4100.00),
(10, 'Eve', 'North', '2025-02-01', 5400.00, 4050.00),
(11, 'Alice', 'North', '2025-03-01', 6500.00, 4000.00),
(12, 'Bob', 'South', '2025-03-01', 7400.00, 4200.00),
(13, 'Charlie', 'East', '2025-03-01', 6000.00, 4300.00),
(14, 'David', 'West', '2025-03-01', 5200.00, 4100.00),
(15, 'Eve', 'North', '2025-03-01', 5600.00, 4050.00),
(16, 'Frank', 'North', '2025-01-01', 4800.00, 3950.00),
(17, 'Grace', 'South', '2025-01-01', 5300.00, 4100.00),
(18, 'Hank', 'East', '2025-01-01', 4700.00, 4150.00),
(19, 'Ivy', 'West', '2025-01-01', 5100.00, 4050.00),
(20, 'Jack', 'North', '2025-01-01', 4950.00, 4000.00),
(21, 'Frank', 'North', '2025-02-01', 4900.00, 3950.00),
(22, 'Grace', 'South', '2025-02-01', 5400.00, 4100.00),
(23, 'Hank', 'East', '2025-02-01', 4800.00, 4150.00),
(24, 'Ivy', 'West', '2025-02-01', 5200.00, 4050.00),
(25, 'Jack', 'North', '2025-02-01', 5050.00, 4000.00),
(26, 'Frank', 'North', '2025-03-01', 5000.00, 3950.00),
(27, 'Grace', 'South', '2025-03-01', 5500.00, 4100.00),
(28, 'Hank', 'East', '2025-03-01', 4900.00, 4150.00),
(29, 'Ivy', 'West', '2025-03-01', 5300.00, 4050.00),
(30, 'Jack', 'North', '2025-03-01', 5150.00, 4000.00),
(31, 'Kelly', 'North', '2025-01-01', 4700.00, 3900.00),
(32, 'Liam', 'South', '2025-01-01', 5600.00, 4200.00),
(33, 'Mona', 'East', '2025-01-01', 5200.00, 4300.00),
(34, 'Nina', 'West', '2025-01-01', 4800.00, 4050.00),
(35, 'Oscar', 'North', '2025-01-01', 4600.00, 3950.00),
(36, 'Kelly', 'North', '2025-02-01', 4800.00, 3900.00),
(37, 'Liam', 'South', '2025-02-01', 5700.00, 4200.00),
(38, 'Mona', 'East', '2025-02-01', 5300.00, 4300.00),
(39, 'Nina', 'West', '2025-02-01', 4900.00, 4050.00),
(40, 'Oscar', 'North', '2025-02-01', 4700.00, 3950.00),
(41, 'Kelly', 'North', '2025-03-01', 4900.00, 3900.00),
(42, 'Liam', 'South', '2025-03-01', 5800.00, 4200.00),
(43, 'Mona', 'East', '2025-03-01', 5400.00, 4300.00),
(44, 'Nina', 'West', '2025-03-01', 5000.00, 4050.00),
(45, 'Oscar', 'North', '2025-03-01', 4800.00, 3950.00),
(46, 'Paul', 'South', '2025-01-01', 6200.00, 4250.00),
(47, 'Quinn', 'East', '2025-01-01', 5900.00, 4100.00),
(48, 'Rita', 'West', '2025-01-01', 5300.00, 4000.00),
(49, 'Steve', 'North', '2025-01-01', 5100.00, 3950.00),
(50, 'Tina', 'South', '2025-01-01', 6000.00,4200.00);


-- COMMAND ----------

--Q!. Rank employees by their sales amount within each month.--
SELECT EmpName,SalesID,SalesAmount,Month,
ROW_NUMBER() OVER(PARTITION BY MONTH ORDER BY SalesAmount DESC)as rank
FROM salesperformance;

-- COMMAND ----------

--2.. Find the employee with the highest sales per region each month.--(with ctes)
SELECT EmpName,SalesID,SalesAmount,region
FROM
(SELECT EmpName,SalesID,SalesAmount,region,
dense_rank() OVER(PARTITION BY region ORDER BY SalesAmount DESC)as rank
FROM salesperformance)aS  saleS_RANK
WHERE RANK=1;

-- COMMAND ----------

--2.. Find the employee with the highest sales per region each month.--(with ctes)
WITH SALES_RANK as(SELECT EmpName,SalesID,SalesAmount,region,
dense_rank() OVER(PARTITION BY region ORDER BY SalesAmount DESC)as rank
FROM salesperformance)

SELECT EmpName,SalesID,SalesAmount,region
FROM saleS_RANK
WHERE RANK=1;


-- COMMAND ----------

--3. Show the sales amount of the previous month for each employEE
select EmpName,SalesID,SalesAmount,LAG(SalesAmount)
over(partition by EmpName order by month)as previous_month_sales
from salesperformance

-- COMMAND ----------

--4. Calculate the cumulative sales amount per employee across all months.
select EmpName,SalesID,SalesAmount,month,sum(SalesAmount)
over(partition by EmpName order by month asc)as cumilative_sales
from salesperformance

-- COMMAND ----------

--.5 Find the difference in sales amount between the current month and the previous month for each employee

select EmpName,SalesID,month,salesAmount,salesAmount-LAG(salesAmount)
over(partition by EmpName order by month asc)as diffrence
from salesperformance;
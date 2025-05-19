CREATE TABLE ProductInventory (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Quantity INT,
    UnitPrice MONEY,
    Supplier VARCHAR(100),
    LastRestocked DATE
);
--
INSERT INTO ProductInventory (ProductID, ProductName, Category, Quantity, UnitPrice, Supplier, LastRestocked)
VALUES 
(1, 'Laptop', 'Electronics', 20, 70000, 'TechMart', '2025-04-20'),
(2, 'Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
(3, 'Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
(4, 'Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
(5, 'Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30');
--1 CRUD Operations
--(1)
INSERT INTO ProductInventory (ProductID, ProductName, Category, Quantity, UnitPrice, Supplier, LastRestocked)
VALUES (6, 'Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01');
--(2)
UPDATE ProductInventory
SET Quantity = Quantity + 20
WHERE ProductName = 'Desk Lamp';
--(3)
DELETE FROM ProductInventory
WHERE ProductID = 2;
--(4)
SELECT * FROM ProductInventory
ORDER BY ProductName ASC;
--2 Sorting And Filtering
--(5)
SELECT * FROM ProductInventory
ORDER BY Quantity DESC;
--(6)
SELECT * FROM ProductInventory
WHERE Category = 'Electronics';
--(7)
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' AND Quantity > 20;
--(8)
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' OR UnitPrice < 2000;
--3 Aggregation And Grouping
--(9)
SELECT SUM(Quantity * UnitPrice) AS TotalStockValue
FROM ProductInventory;
--(10)
SELECT Category, AVG(UnitPrice) AS AvgPrice
FROM ProductInventory
GROUP BY Category;
--(11)
SELECT COUNT(*) AS ProductCount
FROM ProductInventory
WHERE Supplier = 'GadgetHub';
--4 Conditional And Pattern Matching
--(12)
SELECT * FROM ProductInventory
WHERE ProductName LIKE 'W%';
--(13)
SELECT * FROM ProductInventory
WHERE Supplier = 'GadgetHub' AND UnitPrice > 10000;
--(14)
SELECT * FROM ProductInventory
WHERE UnitPrice BETWEEN 1000 AND 20000;
--5 Advanced Queries
--(15)
SELECT TOP 3 *
FROM ProductInventory
ORDER BY UnitPrice DESC;
--(16)
SELECT * FROM ProductInventory
WHERE LastRestocked >= DATEADD(DAY, -10, GETDATE());
--(17)
SELECT Supplier, SUM(Quantity) AS TotalQuantity
FROM ProductInventory
GROUP BY Supplier;
--(18)
SELECT * FROM ProductInventory
WHERE Quantity < 30;
--6 Join and Subqueries
--(19)
SELECT TOP 1 Supplier, COUNT(*) AS ProductCount
FROM ProductInventory
GROUP BY Supplier
ORDER BY ProductCount DESC;
--(20)
SELECT TOP 1 *, Quantity * UnitPrice AS StockValue
FROM ProductInventory
ORDER BY Quantity * UnitPrice DESC;










 
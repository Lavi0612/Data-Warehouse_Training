CREATE TABLE Product (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(10, 2),
    StockQuantity INT,
    Supplier VARCHAR(100)
);

INSERT INTO Product VALUES
(1, 'Laptop', 'Electronics', 70000, 50, 'TechMart'),
(2, 'Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
(3, 'Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
(4, 'Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
(5, 'Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');

--1 Crud operations
--(1)
INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES (6, 'Gaming Keyboard', 'Electronics', 3500, 150, 'TechMart');
--(2)
UPDATE Product
SET Price = Price * 1.10
WHERE Category = 'Electronics';
--(3)
DELETE FROM Product
WHERE ProductID = 4;
--(4)
SELECT * FROM Product
ORDER BY Price DESC;
--2 Sorting and filtering
--(5)
SELECT * FROM Product
ORDER BY StockQuantity ASC;
--(6)
SELECT * FROM Product
WHERE Category = 'Electronics';
--(7)
SELECT * FROM Product
WHERE Category = 'Electronics' AND Price > 5000;
--(8)
SELECT * FROM Product
WHERE Category = 'Electronics' OR Price < 2000;
--3 Aggregation and Grouping
--(9)
SELECT SUM(Price * StockQuantity) AS TotalStockValue
FROM Product;
--(10)
SELECT Category, AVG(Price) AS AveragePrice
FROM Product
GROUP BY Category;
--(11)
SELECT COUNT(*) AS ProductCount
FROM Product
WHERE Supplier = 'GadgetHub';
--4 Conditional and pattern matching
--(12)
SELECT * FROM Product
WHERE ProductName LIKE '%Wireless%';
--(13)
SELECT * FROM Product
WHERE Supplier IN ('TechMart', 'GadgetHub');
--(14)
SELECT * FROM Product
WHERE Price BETWEEN 1000 AND 20000;
--5 Advanced Queries
--(15)
SELECT * FROM Product
WHERE StockQuantity > (
    SELECT AVG(StockQuantity) FROM Product
);
--(16)
SELECT TOP 3 *
FROM Product
ORDER BY Price DESC;
--(17)
SELECT Supplier, COUNT(*) AS ProductCount
FROM Product
GROUP BY Supplier
HAVING COUNT(*) > 1;
--(18)
SELECT Category,
       COUNT(*) AS NumberOfProducts,
       SUM(Price * StockQuantity) AS TotalStockValue
FROM Product
GROUP BY Category;
--6 Joins And Subqueries
--(19)
SELECT TOP 1 Supplier
FROM Product
GROUP BY Supplier
ORDER BY COUNT(*) DESC;
--(20)
SELECT Category, ProductName, Price
FROM Product p
WHERE Price = (
    SELECT MAX(Price)
    FROM Product
    WHERE Category = p.Category
);





















CREATE TABLE Book (
    BookID INT PRIMARY KEY,
    Title VARCHAR(255),
    Author VARCHAR(255),
    Genre VARCHAR(100),
    Price INT,
    PublishedYear INT,
    Stock INT
);
--
INSERT INTO Book (BookID, Title, Author, Genre, Price, PublishedYear, Stock) VALUES
(1, 'The Alchemist', 'Paulo Coelho', 'Fiction', 300, 1988, 50),
(2, 'Sapiens', 'Yuval Noah Harari', 'Non-Fiction', 500, 2011, 30),
(3, 'Atomic Habits', 'James Clear', 'Self-Help', 400, 2018, 25),
(4, 'Rich Dad Poor Dad', 'Robert Kiyosaki', 'Personal Finance', 350, 1997, 20),
(5, 'The Lean Startup', 'Eric Ries', 'Business', 450, 2011, 15);
--1 CRUD Operations
--(1)
INSERT INTO Book (BookID, Title, Author, Genre, Price, PublishedYear, Stock)
VALUES (6, 'Deep Work', 'Cal Newport', 'Self-Help', 420, 2016, 35);
--(2)
UPDATE Book
SET Price = Price + 50
WHERE Genre = 'Self-Help';
--(3)
DELETE FROM Book
WHERE BookID = 4;
--(4)
SELECT * FROM Book
ORDER BY Title ASC;
--2 Sorting and Filtering
--(5)
SELECT * FROM Book
ORDER BY Price DESC;
--(6)
SELECT * FROM Book
WHERE Genre = 'Fiction';
--(7)
SELECT * FROM Book
WHERE Genre = 'Self-Help' AND Price > 400;
--(8)
SELECT * FROM Book
WHERE Genre = 'Fiction' OR PublishedYear > 2000;
--3 Aggregation and Grouping
--(9)
SELECT SUM(Price * Stock) AS TotalStockValue FROM Book;
--(10)
SELECT Genre, AVG(Price) AS AveragePrice
FROM Book
GROUP BY Genre;
--(11)
SELECT COUNT(*) AS BookCount
FROM Book
WHERE Author = 'Paulo Coelho';
--4 Conditional and Pattern Matching
--(12)
SELECT * FROM Book
WHERE Title LIKE '%The%';
--(13)
SELECT * FROM Book
WHERE Author = 'Yuval Noah Harari' AND Price < 600;
--(14)
SELECT * FROM Book
WHERE Price BETWEEN 300 AND 500;
--5 Advanced Queries
--(15)
SELECT TOP 3 *
FROM Book
ORDER BY Price DESC;
--(16)
SELECT * FROM Book
WHERE PublishedYear < 2000;
--(17)
SELECT Genre, COUNT(*) AS TotalBooks
FROM Book
GROUP BY Genre;
--(18)
SELECT Title, COUNT(*) AS Count
FROM Book
GROUP BY Title
HAVING COUNT(*) > 1;
--6 Join and Subqueries
--(19)
SELECT TOP 1 Author, COUNT(*) AS BookCount
FROM Book
GROUP BY Author
ORDER BY BookCount DESC;
--(20)
SELECT Genre, MIN(PublishedYear) AS OldestYear
FROM Book
GROUP BY Genre;




















CREATE TABLE EmployeeAttendance (
    AttendanceID INT PRIMARY KEY,
    EmployeeName VARCHAR(100),
    Department VARCHAR(50),
    Date DATE,
    Status VARCHAR(20),
    HoursWorked INT
);
--
INSERT INTO EmployeeAttendance (AttendanceID, EmployeeName, Department, Date, Status, HoursWorked)
VALUES 
(1, 'John Doe', 'IT', '2025-05-01', 'Present', 8),
(2, 'Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
(3, 'Ali Khan', 'IT', '2025-05-01', 'Present', 7),
(4, 'Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
(5, 'David Brown', 'Marketing', '2025-05-01', 'Present', 8);
--1 CRUD Operations
--(1)
INSERT INTO EmployeeAttendance (AttendanceID, EmployeeName, Department, Date, Status, HoursWorked)
VALUES (6, 'Neha Sharma', 'Finance', '2025-05-01', 'Present', 8);
--(2)
UPDATE EmployeeAttendance
SET Status = 'Present'
WHERE EmployeeName = 'Riya Patel' AND Date = '2025-05-01';
--(3)
DELETE FROM EmployeeAttendance
WHERE EmployeeName = 'Priya Singh' AND Date = '2025-05-01';
--(4)
SELECT * FROM EmployeeAttendance
ORDER BY EmployeeName ASC;
--2 Sorting And Filtering
--(5)
SELECT * FROM EmployeeAttendance
ORDER BY HoursWorked DESC;
--(6)
SELECT * FROM EmployeeAttendance
WHERE Department = 'IT';
--(7)
SELECT * FROM EmployeeAttendance
WHERE Department = 'IT' AND Status = 'Present';
--(8)
SELECT * FROM EmployeeAttendance
WHERE Status = 'Absent' OR Status = 'Late';
--3 Aggregation And Grouping
--(9)
SELECT Department, SUM(HoursWorked) AS TotalHours
FROM EmployeeAttendance
GROUP BY Department;
--(10)
SELECT AVG(HoursWorked) AS AvgHoursWorked
FROM EmployeeAttendance;
--(11)
SELECT Status, COUNT(*) AS Count
FROM EmployeeAttendance
GROUP BY Status;
--4 Conditional And Pattern Matching
--(12)
SELECT * FROM EmployeeAttendance
WHERE EmployeeName LIKE 'R%';
--(13)
SELECT * FROM EmployeeAttendance
WHERE HoursWorked > 6 AND Status = 'Present';
--(14)
SELECT * FROM EmployeeAttendance
WHERE HoursWorked BETWEEN 6 AND 8;
--5 Advanced Queries
--(15)
SELECT TOP 2 *
FROM EmployeeAttendance
ORDER BY HoursWorked DESC;
--(16)
SELECT * FROM EmployeeAttendance
WHERE HoursWorked < (
    SELECT AVG(HoursWorked) FROM EmployeeAttendance
);
--(17)
SELECT Status, AVG(HoursWorked) AS AvgHours
FROM EmployeeAttendance
GROUP BY Status;
--(18)
SELECT EmployeeName, Date, COUNT(*) AS EntryCount
FROM EmployeeAttendance
GROUP BY EmployeeName, Date
HAVING COUNT(*) > 1;
--6 Join and Subqueries
--(19)
SELECT TOP 1 Department, COUNT(*) AS PresentCount
FROM EmployeeAttendance
WHERE Status = 'Present'
GROUP BY Department
ORDER BY PresentCount DESC;
--(20)
SELECT * FROM EmployeeAttendance E WHERE HoursWorked = (
    SELECT MAX(HoursWorked)
    FROM EmployeeAttendance
    WHERE Department = E.Department
);















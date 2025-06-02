Create database OrderTrackingdb;
Use OrderTrackingdb;

-- customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20)
);

-- orders table
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    delivery_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

--  delivery_status table
CREATE TABLE delivery_status (
    status_id INT PRIMARY KEY,
    order_id INT,
    status ENUM('Delivered', 'In Transit', 'Delayed'),
    issue VARCHAR(255),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

Select * from customers;
Select * from orders;
Select * from delivery_status;
 
-- Basic CRUD on orders table

-- INSERT
INSERT INTO orders (order_id, customer_id, order_date, delivery_date)
VALUES (101, 5, '2024-05-01', '2024-05-07');

-- READ
SELECT * FROM orders;

-- UPDATE
UPDATE orders
SET delivery_date = '2024-05-10'
WHERE order_id = 101;

-- DELETE
DELETE FROM orders
WHERE order_id = 101;

-- Stored Procedure to get delayed deliveries for a specific customer
CREATE PROCEDURE GetDelayedDeliveries
    @cust_id INT
AS
BEGIN
    SELECT 
        o.order_id,
        o.order_date,
        o.delivery_date,
        d.status,
        d.issue
    FROM 
        orders o
    INNER JOIN 
        delivery_status d ON o.order_id = d.order_id
    WHERE 
        o.customer_id = @cust_id
        AND d.status = 'Delayed';
END
GO


EXEC GetDelayedDeliveries @cust_id = 5;




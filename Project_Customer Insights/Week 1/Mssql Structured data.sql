Create database OrderTrackingdb;
Use OrderTrackingdb;
SELECT * FROM customers;
SELECT * FROM orders;
SELECT * FROM delivery_status;

-- CRUD on orders
-- CREATE
INSERT INTO orders (order_id, customer_id, order_date, amount)
VALUES (131, 1, '2024-06-01', 299.99);

-- READ
SELECT * FROM orders WHERE customer_id = 1;

-- UPDATE
UPDATE orders SET amount = 349.99 WHERE order_id = 101;

-- DELETE
DELETE FROM orders WHERE order_id = 130;

-- Sample insert into delivery_status
INSERT INTO delivery_status (delivery_id, order_id, status, expected_delivery, actual_delivery)
VALUES (1031, 132, 'Delivered', '2024-06-05', '2024-06-07');

-- Stored Procedure to fetch delayed deliveries
CREATE OR ALTER PROCEDURE GetDelayedDeliveries
    @cust_id INT
AS
BEGIN
    SELECT o.order_id, ds.status, ds.expected_delivery, ds.actual_delivery
    FROM orders o
    JOIN delivery_status ds ON o.order_id = ds.order_id
    WHERE o.customer_id = @cust_id
      AND ds.actual_delivery > ds.expected_delivery;
END;

SELECT * FROM delivery_status WHERE order_id = 101;

-- Execute to test:
EXEC GetDelayedDeliveries @cust_id = 3;



create database supply_chain;
use supply_chain;

CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    contact_email VARCHAR(100),
    phone VARCHAR(20)
);

CREATE TABLE inventory (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(100),
    supplier_id INT,
    quantity INT DEFAULT 0,
    reorder_level INT DEFAULT 10,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT,
    order_date DATE,
    delivery_date DATE,
    quantity INT,
    status VARCHAR(50),
    FOREIGN KEY (product_id) REFERENCES inventory(product_id)
);

ALTER TABLE suppliers AUTO_INCREMENT = 1;
ALTER TABLE inventory AUTO_INCREMENT = 1;
ALTER TABLE orders AUTO_INCREMENT = 1;

INSERT INTO suppliers (name, contact_email, phone) VALUES
('Alpha Supplies', 'contact@alphasupplies.com', '123-456-7890'),
('Beta Traders', 'info@betatraders.com', '234-567-8901'),
('Gamma Corp', 'support@gammacorp.com', '345-678-9012'),
('Delta Distributors', 'sales@deltadistributors.com', '456-789-0123'),
('Epsilon Exporters', 'export@epsilon.com', '567-890-1234'),
('Zeta Logistics', 'zeta@logistics.com', '678-901-2345'),
('Eta Goods', 'eta@goods.com', '789-012-3456'),
('Theta Manufacturing', 'theta@mfg.com', '890-123-4567'),
('Iota Wholesale', 'iota@wholesale.com', '901-234-5678'),
('Kappa Supply Co.', 'kappa@supplyco.com', '012-345-6789');

INSERT INTO inventory (product_name, quantity, reorder_level, supplier_id) VALUES
('Steel Rods', 300, 100, 1),
('Aluminum Sheets', 450, 150, 2),
('Copper Wires', 250, 100, 3),
('Plastic Tubes', 600, 200, 4),
('Rubber Gaskets', 120, 80, 5),
('Glass Panels', 100, 50, 6),
('Iron Bolts', 800, 300, 7),
('Ceramic Tiles', 200, 100, 8),
('PVC Pipes', 700, 250, 9),
('Wooden Planks', 500, 200, 10);

INSERT INTO orders (product_id, order_date, delivery_date, quantity, status) VALUES
(1, '2024-05-01', '2024-05-06', 100, 'Delivered'),
(2, '2024-05-02', '2024-05-12', 150, 'Pending'),
(3, '2024-05-05', '2024-05-15', 80, 'Delivered'),
(4, '2024-05-07', '2024-05-16', 300, 'Shipped'),
(5, '2024-05-09', '2024-05-14', 60, 'Cancelled'),
(6, '2024-05-10', '2024-05-18', 50, 'Delivered'),
(7, '2024-05-11', '2024-05-21', 120, 'Pending'),
(8, '2024-05-13', '2024-05-19', 70, 'Shipped'),
(9, '2024-05-14', '2024-05-20', 250, 'Delivered'),
(10, '2024-05-15', '2024-05-25', 90, 'Pending');

SELECT * FROM orders WHERE status = 'Pending';

UPDATE orders SET status = 'Delivered' WHERE order_id = 3;

SELECT o.order_id, i.product_name, o.quantity, o.status, s.name AS supplier_name
FROM orders o
JOIN inventory i ON o.product_id = i.product_id
JOIN suppliers s ON i.supplier_id = s.supplier_id;

DELIMITER $$

CREATE PROCEDURE AutoReorder()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE p_id INT;
    DECLARE p_name VARCHAR(255);
    DECLARE qty INT;
    DECLARE reorder INT;

    DECLARE reorder_cursor CURSOR FOR 
        SELECT product_id, product_name, quantity, reorder_level FROM inventory;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN reorder_cursor;

    read_loop: LOOP
        FETCH reorder_cursor INTO p_id, p_name, qty, reorder;
        IF done THEN
            LEAVE read_loop;
        END IF;

        IF qty < reorder THEN
            INSERT INTO orders (product_id, order_date, delivery_date, quantity, status)
            VALUES (p_id, CURDATE(), DATE_ADD(CURDATE(), INTERVAL 7 DAY), reorder * 2, 'Auto-Reordered');
        END IF;
    END LOOP;

    CLOSE reorder_cursor;
END$$

DELIMITER ;




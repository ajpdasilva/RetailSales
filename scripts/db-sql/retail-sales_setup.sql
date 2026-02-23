-- 1. Create Database
CREATE DATABASE IF NOT EXISTS `retailsales_db`
CHARACTER SET utf8mb4;

USE retailsales_db;

-- 2. Create Tables
CREATE TABLE IF NOT EXISTS `customers` (
  `customer_id` int NOT NULL,
  `first_name` varchar(45) DEFAULT NULL,
  `last_name` varchar(45) DEFAULT NULL,
  `full_name` varchar(100) DEFAULT NULL,
  `gender` char(1) DEFAULT NULL,
  `age` int DEFAULT NULL,
  `email` varchar(75) DEFAULT NULL,
  `signup_date` date DEFAULT NULL,
  PRIMARY KEY (`customer_id`)
) ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `category` (
  `category_id` int NOT NULL,
  `category_name` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`category_id`)
) ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `products` (
  `product_id` int NOT NULL,
  `product_name` varchar(75) DEFAULT NULL,
  PRIMARY KEY (`product_id`)
) ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS `sales` (
  `transaction_id` int NOT NULL,
  `sale_date` date NOT NULL,
  `sale_time` time NOT NULL,
  `customer_id` int DEFAULT NULL,
  `product_id` int DEFAULT NULL,
  `category_id` int DEFAULT NULL,
  `quantity` int DEFAULT NULL,
  `unit_price` decimal(7,2) DEFAULT NULL,
  `total_sale` decimal(9,2) DEFAULT NULL,
  PRIMARY KEY (`transaction_id`),
  KEY `fk_customer` (`customer_id`),
  KEY `fk_category` (`category_id`),
  KEY `fk_products` (`product_id`),
  CONSTRAINT `fk_category` FOREIGN KEY (`category_id`) REFERENCES `category` (`category_id`),
  CONSTRAINT `fk_customer` FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`),
  CONSTRAINT `fk_products` FOREIGN KEY (`product_id`) REFERENCES `products` (`product_id`)
) ENGINE=InnoDB 
DEFAULT CHARSET=utf8mb4;


-- 3. Create user for database access
CREATE USER IF NOT EXISTS 'etl_user'@'localhost' IDENTIFIED BY '2much4u2d0';

-- 4. Grant user full access permisssions to database
GRANT ALL PRIVILEGES ON retailsales_db.* TO 'etl_user'@'localhost';
-- GRANT ALL PRIVILEGES ON retailsales_db.* TO 'etl_user'@'%';

-- OR Grant specific privileges only (least privilege recommended)
-- GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX
-- ON retailsales_db.*
-- TO 'etl_user'@'localhost';

-- GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX
-- ON retailsales_db.*
-- TO 'etl_user'@'%';

-- 5. Apply Changes
FLUSH PRIVILEGES;

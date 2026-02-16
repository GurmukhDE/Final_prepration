-- ===================================================================
-- NAB DATA ENGINEER INTERVIEW - MYSQL DATABASE SETUP
-- ===================================================================
-- This script creates all tables and inserts sample data
-- for practicing SQL interview questions
-- 
-- Run this script in your MySQL database to set up everything
-- ===================================================================

-- Create database (optional - if you want a new database)
CREATE DATABASE IF NOT EXISTS nab_practice;
USE nab_practice;

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS product_categories;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS employees;

-- ===================================================================
-- TABLE 1: CUSTOMERS
-- ===================================================================
CREATE TABLE customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    signup_date DATE,
    customer_type VARCHAR(20) -- 'Premium', 'Regular', 'Basic'
);

-- Insert sample customers (50 records)
INSERT INTO customers VALUES
('CUST001', 'John Smith', 'john.smith@email.com', 'New York', 'USA', '2023-01-15', 'Premium'),
('CUST002', 'Sarah Johnson', 'sarah.j@email.com', 'Los Angeles', 'USA', '2023-02-20', 'Regular'),
('CUST003', 'Michael Brown', 'mbrown@email.com', 'Chicago', 'USA', '2023-03-10', 'Premium'),
('CUST004', 'Emily Davis', 'emily.d@email.com', 'Houston', 'USA', '2023-01-25', 'Basic'),
('CUST005', 'David Wilson', 'dwilson@email.com', 'Phoenix', 'USA', '2023-04-05', 'Regular'),
('CUST006', 'Lisa Anderson', 'lisa.a@email.com', 'Philadelphia', 'USA', '2023-02-14', 'Premium'),
('CUST007', 'James Martinez', 'james.m@email.com', 'San Antonio', 'USA', '2023-05-20', 'Regular'),
('CUST008', 'Jennifer Taylor', 'jennifer.t@email.com', 'San Diego', 'USA', '2023-03-30', 'Basic'),
('CUST009', 'Robert Thomas', 'robert.t@email.com', 'Dallas', 'USA', '2023-01-08', 'Premium'),
('CUST010', 'Mary Jackson', 'mary.j@email.com', 'San Jose', 'USA', '2023-06-12', 'Regular'),
('CUST011', 'William White', 'william.w@email.com', 'Austin', 'USA', '2023-02-28', 'Premium'),
('CUST012', 'Patricia Harris', 'patricia.h@email.com', 'Jacksonville', 'USA', '2023-04-15', 'Basic'),
('CUST013', 'Christopher Martin', 'chris.m@email.com', 'San Francisco', 'USA', '2023-01-20', 'Premium'),
('CUST014', 'Linda Thompson', 'linda.t@email.com', 'Columbus', 'USA', '2023-03-25', 'Regular'),
('CUST015', 'Daniel Garcia', 'daniel.g@email.com', 'Charlotte', 'USA', '2023-05-10', 'Premium'),
('CUST016', 'Barbara Martinez', 'barbara.m@email.com', 'Seattle', 'USA', '2023-02-05', 'Basic'),
('CUST017', 'Matthew Robinson', 'matthew.r@email.com', 'Denver', 'USA', '2023-04-22', 'Regular'),
('CUST018', 'Elizabeth Clark', 'elizabeth.c@email.com', 'Boston', 'USA', '2023-01-30', 'Premium'),
('CUST019', 'Anthony Rodriguez', 'anthony.r@email.com', 'Nashville', 'USA', '2023-06-08', 'Regular'),
('CUST020', 'Susan Lewis', 'susan.l@email.com', 'Portland', 'USA', '2023-03-18', 'Basic'),
('CUST021', 'Mark Lee', 'mark.lee@email.com', 'Las Vegas', 'USA', '2023-02-12', 'Premium'),
('CUST022', 'Nancy Walker', 'nancy.w@email.com', 'Detroit', 'USA', '2023-05-05', 'Regular'),
('CUST023', 'Donald Hall', 'donald.h@email.com', 'Memphis', 'USA', '2023-01-18', 'Premium'),
('CUST024', 'Karen Allen', 'karen.a@email.com', 'Baltimore', 'USA', '2023-04-08', 'Basic'),
('CUST025', 'Steven Young', 'steven.y@email.com', 'Milwaukee', 'USA', '2023-03-22', 'Regular'),
('CUST026', 'Betty Hernandez', 'betty.h@email.com', 'Albuquerque', 'USA', '2023-06-15', 'Premium'),
('CUST027', 'Edward King', 'edward.k@email.com', 'Tucson', 'USA', '2023-02-20', 'Regular'),
('CUST028', 'Helen Wright', 'helen.w@email.com', 'Fresno', 'USA', '2023-05-12', 'Basic'),
('CUST029', 'Brian Lopez', 'brian.l@email.com', 'Sacramento', 'USA', '2023-01-25', 'Premium'),
('CUST030', 'Dorothy Hill', 'dorothy.h@email.com', 'Kansas City', 'USA', '2023-04-18', 'Regular'),
('CUST031', 'Jason Scott', 'jason.s@email.com', 'Atlanta', 'USA', '2023-03-08', 'Premium'),
('CUST032', 'Sandra Green', 'sandra.g@email.com', 'Miami', 'USA', '2023-06-20', 'Basic'),
('CUST033', 'Ryan Adams', 'ryan.a@email.com', 'Minneapolis', 'USA', '2023-02-15', 'Regular'),
('CUST034', 'Ashley Baker', 'ashley.b@email.com', 'Cleveland', 'USA', '2023-05-22', 'Premium'),
('CUST035', 'Justin Gonzalez', 'justin.g@email.com', 'New Orleans', 'USA', '2023-01-12', 'Regular'),
('CUST036', 'Kimberly Nelson', 'kimberly.n@email.com', 'Tampa', 'USA', '2023-04-25', 'Basic'),
('CUST037', 'Brandon Carter', 'brandon.c@email.com', 'Honolulu', 'USA', '2023-03-15', 'Premium'),
('CUST038', 'Melissa Mitchell', 'melissa.m@email.com', 'Anaheim', 'USA', '2023-06-05', 'Regular'),
('CUST039', 'Kevin Perez', 'kevin.p@email.com', 'Santa Ana', 'USA', '2023-02-08', 'Premium'),
('CUST040', 'Stephanie Roberts', 'stephanie.r@email.com', 'St. Louis', 'USA', '2023-05-18', 'Basic'),
('CUST041', 'Jacob Turner', 'jacob.t@email.com', 'Pittsburgh', 'USA', '2023-01-22', 'Regular'),
('CUST042', 'Nicole Phillips', 'nicole.p@email.com', 'Cincinnati', 'USA', '2023-04-12', 'Premium'),
('CUST043', 'Gary Campbell', 'gary.c@email.com', 'Riverside', 'USA', '2023-03-28', 'Regular'),
('CUST044', 'Rebecca Parker', 'rebecca.p@email.com', 'Stockton', 'USA', '2023-06-10', 'Basic'),
('CUST045', 'Nicholas Evans', 'nicholas.e@email.com', 'Corpus Christi', 'USA', '2023-02-18', 'Premium'),
('CUST046', 'Pamela Edwards', 'pamela.e@email.com', 'Lexington', 'USA', '2023-05-08', 'Regular'),
('CUST047', 'Samuel Collins', 'samuel.c@email.com', 'Henderson', 'USA', '2023-01-28', 'Premium'),
('CUST048', 'Cynthia Stewart', 'cynthia.s@email.com', 'Plano', 'USA', '2023-04-20', 'Basic'),
('CUST049', 'Jeremy Sanchez', 'jeremy.s@email.com', 'Lincoln', 'USA', '2023-03-12', 'Regular'),
('CUST050', 'Kathleen Morris', 'kathleen.m@email.com', 'Orlando', 'USA', '2023-06-25', 'Premium');

-- ===================================================================
-- TABLE 2: TRANSACTIONS
-- ===================================================================
CREATE TABLE transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    amount DECIMAL(10,2),
    transaction_date DATE,
    transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    merchant VARCHAR(100),
    category VARCHAR(50),
    status VARCHAR(20), -- 'Completed', 'Pending', 'Failed'
    card_id VARCHAR(20),
    city VARCHAR(50),
    country_code VARCHAR(5),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Insert sample transactions (100+ records spanning multiple months)
INSERT INTO transactions VALUES
-- January 2024
('TXN001', 'CUST001', 1250.00, '2024-01-05', '2024-01-05 10:30:00', 'Amazon', 'Shopping', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN002', 'CUST002', 75.50, '2024-01-05', '2024-01-05 14:20:00', 'Starbucks', 'Food', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN003', 'CUST003', 15000.00, '2024-01-06', '2024-01-06 09:15:00', 'Tesla', 'Automotive', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN004', 'CUST001', 250.00, '2024-01-06', '2024-01-06 16:45:00', 'Target', 'Shopping', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN005', 'CUST004', 89.99, '2024-01-07', '2024-01-07 11:30:00', 'Best Buy', 'Electronics', 'Completed', 'CARD004', 'Houston', 'USA'),
('TXN006', 'CUST005', 12500.00, '2024-01-08', '2024-01-08 13:20:00', 'Apple Store', 'Electronics', 'Completed', 'CARD005', 'Phoenix', 'USA'),
('TXN007', 'CUST006', 450.00, '2024-01-10', '2024-01-10 15:10:00', 'Walmart', 'Shopping', 'Completed', 'CARD006', 'Philadelphia', 'USA'),
('TXN008', 'CUST002', 35.00, '2024-01-10', '2024-01-10 19:30:00', 'McDonalds', 'Food', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN009', 'CUST007', 8900.00, '2024-01-12', '2024-01-12 10:00:00', 'Best Buy', 'Electronics', 'Completed', 'CARD007', 'San Antonio', 'USA'),
('TXN010', 'CUST008', 125.75, '2024-01-15', '2024-01-15 12:45:00', 'Target', 'Shopping', 'Completed', 'CARD008', 'San Diego', 'USA'),
('TXN011', 'CUST009', 20000.00, '2024-01-18', '2024-01-18 14:30:00', 'Jewelry Store', 'Luxury', 'Completed', 'CARD009', 'Dallas', 'USA'),
('TXN012', 'CUST010', 67.50, '2024-01-20', '2024-01-20 18:15:00', 'Chipotle', 'Food', 'Completed', 'CARD010', 'San Jose', 'USA'),
('TXN013', 'CUST011', 5500.00, '2024-01-22', '2024-01-22 09:30:00', 'Macys', 'Shopping', 'Completed', 'CARD011', 'Austin', 'USA'),
('TXN014', 'CUST003', 180.00, '2024-01-25', '2024-01-25 16:00:00', 'Gas Station', 'Fuel', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN015', 'CUST001', 3200.00, '2024-01-28', '2024-01-28 11:20:00', 'Hotels.com', 'Travel', 'Completed', 'CARD001', 'New York', 'USA'),

-- February 2024
('TXN016', 'CUST012', 95.00, '2024-02-01', '2024-02-01 10:15:00', 'Whole Foods', 'Groceries', 'Completed', 'CARD012', 'Jacksonville', 'USA'),
('TXN017', 'CUST013', 11500.00, '2024-02-02', '2024-02-02 13:45:00', 'Apple Store', 'Electronics', 'Completed', 'CARD013', 'San Francisco', 'USA'),
('TXN018', 'CUST001', 450.00, '2024-02-03', '2024-02-03 15:30:00', 'Amazon', 'Shopping', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN019', 'CUST014', 230.50, '2024-02-05', '2024-02-05 12:00:00', 'Target', 'Shopping', 'Completed', 'CARD014', 'Columbus', 'USA'),
('TXN020', 'CUST015', 18000.00, '2024-02-08', '2024-02-08 10:30:00', 'Furniture Store', 'Home', 'Completed', 'CARD015', 'Charlotte', 'USA'),
('TXN021', 'CUST002', 85.00, '2024-02-10', '2024-02-10 19:00:00', 'Restaurant', 'Food', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN022', 'CUST016', 45.99, '2024-02-12', '2024-02-12 14:20:00', 'CVS', 'Pharmacy', 'Completed', 'CARD016', 'Seattle', 'USA'),
('TXN023', 'CUST017', 7800.00, '2024-02-15', '2024-02-15 11:45:00', 'Best Buy', 'Electronics', 'Completed', 'CARD017', 'Denver', 'USA'),
('TXN024', 'CUST003', 320.00, '2024-02-18', '2024-02-18 16:30:00', 'Costco', 'Shopping', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN025', 'CUST018', 25000.00, '2024-02-20', '2024-02-20 09:00:00', 'Rolex', 'Luxury', 'Completed', 'CARD018', 'Boston', 'USA'),
('TXN026', 'CUST019', 125.00, '2024-02-22', '2024-02-22 13:15:00', 'Nike Store', 'Apparel', 'Completed', 'CARD019', 'Nashville', 'USA'),
('TXN027', 'CUST001', 890.00, '2024-02-25', '2024-02-25 15:00:00', 'BestBuy', 'Electronics', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN028', 'CUST020', 67.50, '2024-02-28', '2024-02-28 18:45:00', 'Subway', 'Food', 'Completed', 'CARD020', 'Portland', 'USA'),

-- March 2024
('TXN029', 'CUST021', 15500.00, '2024-03-02', '2024-03-02 10:30:00', 'Louis Vuitton', 'Luxury', 'Completed', 'CARD021', 'Las Vegas', 'USA'),
('TXN030', 'CUST022', 189.99, '2024-03-05', '2024-03-05 12:20:00', 'Home Depot', 'Home', 'Completed', 'CARD022', 'Detroit', 'USA'),
('TXN031', 'CUST001', 2100.00, '2024-03-08', '2024-03-08 14:00:00', 'Delta Airlines', 'Travel', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN032', 'CUST023', 8700.00, '2024-03-10', '2024-03-10 11:15:00', 'Apple Store', 'Electronics', 'Completed', 'CARD023', 'Memphis', 'USA'),
('TXN033', 'CUST024', 95.00, '2024-03-12', '2024-03-12 16:45:00', 'Whole Foods', 'Groceries', 'Completed', 'CARD024', 'Baltimore', 'USA'),
('TXN034', 'CUST025', 450.00, '2024-03-15', '2024-03-15 13:30:00', 'Target', 'Shopping', 'Completed', 'CARD025', 'Milwaukee', 'USA'),
('TXN035', 'CUST003', 670.00, '2024-03-18', '2024-03-18 15:20:00', 'Amazon', 'Shopping', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN036', 'CUST026', 12000.00, '2024-03-20', '2024-03-20 10:00:00', 'Tiffany', 'Luxury', 'Completed', 'CARD026', 'Albuquerque', 'USA'),
('TXN037', 'CUST027', 230.00, '2024-03-22', '2024-03-22 17:00:00', 'Costco', 'Shopping', 'Completed', 'CARD027', 'Tucson', 'USA'),
('TXN038', 'CUST002', 95.50, '2024-03-25', '2024-03-25 19:15:00', 'Restaurant', 'Food', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN039', 'CUST028', 78.99, '2024-03-28', '2024-03-28 12:30:00', 'CVS', 'Pharmacy', 'Completed', 'CARD028', 'Fresno', 'USA'),

-- April 2024
('TXN040', 'CUST029', 19500.00, '2024-04-02', '2024-04-02 11:00:00', 'Furniture Store', 'Home', 'Completed', 'CARD029', 'Sacramento', 'USA'),
('TXN041', 'CUST001', 1850.00, '2024-04-05', '2024-04-05 14:30:00', 'Hotels.com', 'Travel', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN042', 'CUST030', 345.00, '2024-04-08', '2024-04-08 16:15:00', 'Nike Store', 'Apparel', 'Completed', 'CARD030', 'Kansas City', 'USA'),
('TXN043', 'CUST031', 22000.00, '2024-04-10', '2024-04-10 09:45:00', 'Gucci', 'Luxury', 'Completed', 'CARD031', 'Atlanta', 'USA'),
('TXN044', 'CUST032', 125.00, '2024-04-12', '2024-04-12 13:00:00', 'Walmart', 'Shopping', 'Completed', 'CARD032', 'Miami', 'USA'),
('TXN045', 'CUST003', 980.00, '2024-04-15', '2024-04-15 15:45:00', 'BestBuy', 'Electronics', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN046', 'CUST033', 567.50, '2024-04-18', '2024-04-18 12:20:00', 'Target', 'Shopping', 'Completed', 'CARD033', 'Minneapolis', 'USA'),
('TXN047', 'CUST034', 16500.00, '2024-04-20', '2024-04-20 10:30:00', 'Apple Store', 'Electronics', 'Completed', 'CARD034', 'Cleveland', 'USA'),
('TXN048', 'CUST002', 89.00, '2024-04-22', '2024-04-22 18:00:00', 'Chipotle', 'Food', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN049', 'CUST035', 234.99, '2024-04-25', '2024-04-25 14:45:00', 'Home Depot', 'Home', 'Completed', 'CARD035', 'New Orleans', 'USA'),
('TXN050', 'CUST001', 3500.00, '2024-04-28', '2024-04-28 11:30:00', 'Amazon', 'Shopping', 'Completed', 'CARD001', 'New York', 'USA'),

-- May 2024
('TXN051', 'CUST036', 145.00, '2024-05-02', '2024-05-02 13:15:00', 'Whole Foods', 'Groceries', 'Completed', 'CARD036', 'Tampa', 'USA'),
('TXN052', 'CUST037', 28000.00, '2024-05-05', '2024-05-05 09:30:00', 'Cartier', 'Luxury', 'Completed', 'CARD037', 'Honolulu', 'USA'),
('TXN053', 'CUST003', 1200.00, '2024-05-08', '2024-05-08 15:00:00', 'Delta Airlines', 'Travel', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN054', 'CUST038', 456.00, '2024-05-10', '2024-05-10 12:45:00', 'Target', 'Shopping', 'Completed', 'CARD038', 'Anaheim', 'USA'),
('TXN055', 'CUST039', 9800.00, '2024-05-12', '2024-05-12 10:20:00', 'BestBuy', 'Electronics', 'Completed', 'CARD039', 'Santa Ana', 'USA'),
('TXN056', 'CUST002', 78.50, '2024-05-15', '2024-05-15 19:30:00', 'Restaurant', 'Food', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN057', 'CUST040', 89.99, '2024-05-18', '2024-05-18 14:00:00', 'CVS', 'Pharmacy', 'Completed', 'CARD040', 'St. Louis', 'USA'),
('TXN058', 'CUST001', 2800.00, '2024-05-20', '2024-05-20 11:15:00', 'Hotels.com', 'Travel', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN059', 'CUST041', 345.00, '2024-05-22', '2024-05-22 16:30:00', 'Nike Store', 'Apparel', 'Completed', 'CARD041', 'Pittsburgh', 'USA'),
('TXN060', 'CUST042', 17500.00, '2024-05-25', '2024-05-25 09:45:00', 'Furniture Store', 'Home', 'Completed', 'CARD042', 'Cincinnati', 'USA'),

-- June 2024
('TXN061', 'CUST043', 567.00, '2024-06-02', '2024-06-02 13:20:00', 'Costco', 'Shopping', 'Completed', 'CARD043', 'Riverside', 'USA'),
('TXN062', 'CUST003', 1450.00, '2024-06-05', '2024-06-05 15:45:00', 'Amazon', 'Shopping', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN063', 'CUST044', 123.50, '2024-06-08', '2024-06-08 12:00:00', 'Walmart', 'Shopping', 'Completed', 'CARD044', 'Stockton', 'USA'),
('TXN064', 'CUST045', 24000.00, '2024-06-10', '2024-06-10 10:30:00', 'Rolex', 'Luxury', 'Completed', 'CARD045', 'Corpus Christi', 'USA'),
('TXN065', 'CUST046', 289.99, '2024-06-12', '2024-06-12 14:15:00', 'Target', 'Shopping', 'Completed', 'CARD046', 'Lexington', 'USA'),
('TXN066', 'CUST002', 95.00, '2024-06-15', '2024-06-15 18:45:00', 'Chipotle', 'Food', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN067', 'CUST047', 11200.00, '2024-06-18', '2024-06-18 11:00:00', 'Apple Store', 'Electronics', 'Completed', 'CARD047', 'Henderson', 'USA'),
('TXN068', 'CUST001', 4200.00, '2024-06-20', '2024-06-20 09:30:00', 'Delta Airlines', 'Travel', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN069', 'CUST048', 178.00, '2024-06-22', '2024-06-22 16:00:00', 'Home Depot', 'Home', 'Completed', 'CARD048', 'Plano', 'USA'),
('TXN070', 'CUST049', 456.50, '2024-06-25', '2024-06-25 13:45:00', 'Nike Store', 'Apparel', 'Completed', 'CARD049', 'Lincoln', 'USA'),

-- More recent transactions (July-August 2024)
('TXN071', 'CUST050', 19800.00, '2024-07-02', '2024-07-02 10:15:00', 'Louis Vuitton', 'Luxury', 'Completed', 'CARD050', 'Orlando', 'USA'),
('TXN072', 'CUST001', 1850.00, '2024-07-05', '2024-07-05 14:30:00', 'Amazon', 'Shopping', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN073', 'CUST003', 2300.00, '2024-07-08', '2024-07-08 11:45:00', 'BestBuy', 'Electronics', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN074', 'CUST002', 125.00, '2024-07-10', '2024-07-10 19:00:00', 'Restaurant', 'Food', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN075', 'CUST013', 15600.00, '2024-07-12', '2024-07-12 09:30:00', 'Apple Store', 'Electronics', 'Completed', 'CARD013', 'San Francisco', 'USA'),
('TXN076', 'CUST001', 5600.00, '2024-08-05', '2024-08-05 13:20:00', 'Hotels.com', 'Travel', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN077', 'CUST003', 3400.00, '2024-08-10', '2024-08-10 15:15:00', 'Amazon', 'Shopping', 'Completed', 'CARD003', 'Chicago', 'USA'),
('TXN078', 'CUST002', 156.00, '2024-08-15', '2024-08-15 18:30:00', 'Whole Foods', 'Groceries', 'Completed', 'CARD002', 'Los Angeles', 'USA'),
('TXN079', 'CUST009', 27000.00, '2024-08-20', '2024-08-20 10:00:00', 'Cartier', 'Luxury', 'Completed', 'CARD009', 'Dallas', 'USA'),
('TXN080', 'CUST001', 2100.00, '2024-08-25', '2024-08-25 12:45:00', 'Delta Airlines', 'Travel', 'Completed', 'CARD001', 'New York', 'USA'),

-- Add some failed/pending transactions for practice
('TXN081', 'CUST005', 450.00, '2024-08-28', '2024-08-28 14:20:00', 'Amazon', 'Shopping', 'Failed', 'CARD005', 'Phoenix', 'USA'),
('TXN082', 'CUST010', 89.50, '2024-08-29', '2024-08-29 16:30:00', 'Target', 'Shopping', 'Pending', 'CARD010', 'San Jose', 'USA'),

-- Transactions for fraud detection practice (same card, different cities, within 1 hour)
('TXN083', 'CUST001', 250.00, '2024-08-30', '2024-08-30 10:00:00', 'Store A', 'Shopping', 'Completed', 'CARD001', 'New York', 'USA'),
('TXN084', 'CUST001', 180.00, '2024-08-30', '2024-08-30 10:30:00', 'Store B', 'Shopping', 'Completed', 'CARD001', 'Los Angeles', 'USA');

-- ===================================================================
-- TABLE 3: ACCOUNTS
-- ===================================================================
CREATE TABLE accounts (
    account_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    account_type VARCHAR(20), -- 'Checking', 'Savings', 'Credit'
    balance DECIMAL(12,2),
    opening_date DATE,
    status VARCHAR(20), -- 'Active', 'Closed', 'Frozen'
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Insert sample accounts
INSERT INTO accounts VALUES
('ACC001', 'CUST001', 'Checking', 15000.00, '2023-01-15', 'Active'),
('ACC002', 'CUST001', 'Savings', 45000.00, '2023-01-15', 'Active'),
('ACC003', 'CUST002', 'Checking', 8500.50, '2023-02-20', 'Active'),
('ACC004', 'CUST003', 'Checking', 25000.00, '2023-03-10', 'Active'),
('ACC005', 'CUST003', 'Savings', 120000.00, '2023-03-10', 'Active'),
('ACC006', 'CUST004', 'Checking', 3200.00, '2023-01-25', 'Active'),
('ACC007', 'CUST005', 'Checking', 12000.00, '2023-04-05', 'Active'),
('ACC008', 'CUST006', 'Checking', 18500.00, '2023-02-14', 'Active'),
('ACC009', 'CUST006', 'Savings', 85000.00, '2023-02-14', 'Active'),
('ACC010', 'CUST007', 'Checking', -250.00, '2023-05-20', 'Active'),
('ACC011', 'CUST008', 'Checking', 5600.00, '2023-03-30', 'Active'),
('ACC012', 'CUST009', 'Checking', 32000.00, '2023-01-08', 'Active'),
('ACC013', 'CUST009', 'Savings', 150000.00, '2023-01-08', 'Active'),
('ACC014', 'CUST010', 'Checking', 9800.00, '2023-06-12', 'Active'),
('ACC015', 'CUST011', 'Checking', 21000.00, '2023-02-28', 'Active'),
('ACC016', 'CUST012', 'Checking', 4500.00, '2023-04-15', 'Active'),
('ACC017', 'CUST013', 'Checking', 28000.00, '2023-01-20', 'Active'),
('ACC018', 'CUST013', 'Savings', 95000.00, '2023-01-20', 'Active'),
('ACC019', 'CUST014', 'Checking', 7200.00, '2023-03-25', 'Active'),
('ACC020', 'CUST015', 'Checking', 19500.00, '2023-05-10', 'Active'),
('ACC021', 'CUST016', 'Checking', 2800.00, '2023-02-05', 'Active'),
('ACC022', 'CUST017', 'Checking', 11000.00, '2023-04-22', 'Active'),
('ACC023', 'CUST018', 'Checking', 35000.00, '2023-01-30', 'Active'),
('ACC024', 'CUST018', 'Savings', 180000.00, '2023-01-30', 'Active'),
('ACC025', 'CUST019', 'Checking', 8900.00, '2023-06-08', 'Active'),
('ACC026', 'CUST020', 'Checking', 4200.00, '2023-03-18', 'Active'),
('ACC027', 'CUST021', 'Checking', 23000.00, '2023-02-12', 'Active'),
('ACC028', 'CUST022', 'Checking', -150.00, '2023-05-05', 'Active'),
('ACC029', 'CUST023', 'Checking', 27000.00, '2023-01-18', 'Active'),
('ACC030', 'CUST024', 'Checking', 3800.00, '2023-04-08', 'Active');

-- ===================================================================
-- TABLE 4: EMPLOYEES (for hierarchy questions)
-- ===================================================================
CREATE TABLE employees (
    employee_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department VARCHAR(50),
    salary DECIMAL(10,2),
    manager_id VARCHAR(20),
    hire_date DATE,
    FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

-- Insert employees with hierarchy
INSERT INTO employees VALUES
-- CEO (no manager)
('EMP001', 'Robert CEO', 'Executive', 250000.00, NULL, '2020-01-01'),

-- VPs (report to CEO)
('EMP002', 'Sarah VP Engineering', 'Engineering', 180000.00, 'EMP001', '2020-02-01'),
('EMP003', 'Michael VP Sales', 'Sales', 170000.00, 'EMP001', '2020-02-15'),
('EMP004', 'Jennifer VP Finance', 'Finance', 175000.00, 'EMP001', '2020-03-01'),

-- Directors (report to VPs)
('EMP005', 'David Director Data', 'Engineering', 140000.00, 'EMP002', '2020-04-01'),
('EMP006', 'Lisa Director Dev', 'Engineering', 145000.00, 'EMP002', '2020-04-15'),
('EMP007', 'James Director Sales', 'Sales', 135000.00, 'EMP003', '2020-05-01'),
('EMP008', 'Emily Director Accounting', 'Finance', 130000.00, 'EMP004', '2020-05-15'),

-- Managers (report to Directors)
('EMP009', 'John Manager DE', 'Engineering', 110000.00, 'EMP005', '2020-06-01'),
('EMP010', 'Mary Manager Analytics', 'Engineering', 105000.00, 'EMP005', '2020-06-15'),
('EMP011', 'William Manager Backend', 'Engineering', 108000.00, 'EMP006', '2020-07-01'),
('EMP012', 'Patricia Manager Regional', 'Sales', 100000.00, 'EMP007', '2020-07-15'),
('EMP013', 'Richard Manager Payroll', 'Finance', 95000.00, 'EMP008', '2020-08-01'),

-- Individual Contributors
('EMP014', 'Linda Data Engineer', 'Engineering', 95000.00, 'EMP009', '2021-01-15'),
('EMP015', 'Charles Data Engineer', 'Engineering', 92000.00, 'EMP009', '2021-02-01'),
('EMP016', 'Barbara Data Analyst', 'Engineering', 85000.00, 'EMP010', '2021-03-01'),
('EMP017', 'Daniel Backend Dev', 'Engineering', 88000.00, 'EMP011', '2021-04-01'),
('EMP018', 'Nancy Sales Rep', 'Sales', 75000.00, 'EMP012', '2021-05-01'),
('EMP019', 'Steven Accountant', 'Finance', 72000.00, 'EMP013', '2021-06-01'),
('EMP020', 'Karen Data Engineer', 'Engineering', 94000.00, 'EMP009', '2021-07-01');

-- ===================================================================
-- TABLE 5: PRODUCT CATEGORIES
-- ===================================================================
CREATE TABLE product_categories (
    category_id VARCHAR(20) PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL,
    description TEXT
);

INSERT INTO product_categories VALUES
('CAT001', 'Electronics', 'Electronic devices and accessories'),
('CAT002', 'Clothing', 'Apparel and fashion items'),
('CAT003', 'Home & Kitchen', 'Home appliances and kitchenware'),
('CAT004', 'Books', 'Physical and digital books'),
('CAT005', 'Sports', 'Sports equipment and accessories'),
('CAT006', 'Toys', 'Toys and games for all ages'),
('CAT007', 'Beauty', 'Beauty and personal care products'),
('CAT008', 'Automotive', 'Auto parts and accessories'),
('CAT009', 'Grocery', 'Food and grocery items'),
('CAT010', 'Health', 'Health and wellness products');

-- ===================================================================
-- TABLE 6: PRODUCTS
-- ===================================================================
CREATE TABLE products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category_id VARCHAR(20),
    price DECIMAL(10,2),
    stock_quantity INT,
    FOREIGN KEY (category_id) REFERENCES product_categories(category_id)
);

INSERT INTO products VALUES
('PROD001', 'iPhone 14 Pro', 'CAT001', 999.00, 150),
('PROD002', 'MacBook Air M2', 'CAT001', 1199.00, 80),
('PROD003', 'Samsung 4K TV 55"', 'CAT001', 699.00, 45),
('PROD004', 'Sony WH-1000XM5 Headphones', 'CAT001', 399.00, 200),
('PROD005', 'iPad Air', 'CAT001', 599.00, 120),
('PROD006', 'Nike Air Max Shoes', 'CAT002', 129.00, 300),
('PROD007', 'Levis Jeans 501', 'CAT002', 89.00, 250),
('PROD008', 'Adidas T-Shirt', 'CAT002', 35.00, 500),
('PROD009', 'Winter Jacket', 'CAT002', 199.00, 100),
('PROD010', 'Running Shorts', 'CAT002', 45.00, 400),
('PROD011', 'KitchenAid Mixer', 'CAT003', 299.00, 60),
('PROD012', 'Dyson Vacuum V15', 'CAT003', 649.00, 40),
('PROD013', 'Instant Pot Duo', 'CAT003', 89.00, 180),
('PROD014', 'Coffee Maker', 'CAT003', 79.00, 150),
('PROD015', 'Air Fryer', 'CAT003', 119.00, 200),
('PROD016', 'The Great Gatsby', 'CAT004', 15.00, 500),
('PROD017', '1984 by Orwell', 'CAT004', 18.00, 400),
('PROD018', 'Harry Potter Set', 'CAT004', 89.00, 150),
('PROD019', 'Atomic Habits', 'CAT004', 24.00, 300),
('PROD020', 'Educated Memoir', 'CAT004', 22.00, 250),
('PROD021', 'Yoga Mat', 'CAT005', 35.00, 300),
('PROD022', 'Dumbbells Set', 'CAT005', 89.00, 120),
('PROD023', 'Tennis Racket', 'CAT005', 149.00, 80),
('PROD024', 'Basketball', 'CAT005', 29.00, 200),
('PROD025', 'Running Shoes', 'CAT005', 129.00, 250),
('PROD026', 'LEGO Star Wars Set', 'CAT006', 79.00, 100),
('PROD027', 'Barbie Doll House', 'CAT006', 129.00, 80),
('PROD028', 'PS5 Console', 'CAT006', 499.00, 50),
('PROD029', 'Board Game Collection', 'CAT006', 45.00, 150),
('PROD030', 'RC Car', 'CAT006', 89.00, 100);

-- ===================================================================
-- TABLE 7: ORDERS
-- ===================================================================
CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    order_date DATE,
    total_amount DECIMAL(10,2),
    status VARCHAR(20), -- 'Completed', 'Processing', 'Shipped', 'Cancelled'
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

INSERT INTO orders VALUES
('ORD001', 'CUST001', '2024-01-15', 1598.00, 'Completed'),
('ORD002', 'CUST002', '2024-01-16', 258.00, 'Completed'),
('ORD003', 'CUST003', '2024-01-18', 1898.00, 'Completed'),
('ORD004', 'CUST001', '2024-02-05', 434.00, 'Completed'),
('ORD005', 'CUST004', '2024-02-10', 179.00, 'Completed'),
('ORD006', 'CUST005', '2024-02-15', 728.00, 'Completed'),
('ORD007', 'CUST002', '2024-03-01', 324.00, 'Completed'),
('ORD008', 'CUST006', '2024-03-10', 1548.00, 'Completed'),
('ORD009', 'CUST001', '2024-03-20', 693.00, 'Completed'),
('ORD010', 'CUST007', '2024-04-05', 267.00, 'Completed'),
('ORD011', 'CUST003', '2024-04-12', 1278.00, 'Completed'),
('ORD012', 'CUST008', '2024-04-20', 158.00, 'Completed'),
('ORD013', 'CUST002', '2024-05-08', 453.00, 'Completed'),
('ORD014', 'CUST009', '2024-05-15', 2147.00, 'Completed'),
('ORD015', 'CUST001', '2024-05-25', 829.00, 'Completed'),
('ORD016', 'CUST010', '2024-06-02', 394.00, 'Completed'),
('ORD017', 'CUST011', '2024-06-10', 1673.00, 'Completed'),
('ORD018', 'CUST003', '2024-06-18', 924.00, 'Completed'),
('ORD019', 'CUST012', '2024-07-05', 289.00, 'Completed'),
('ORD020', 'CUST001', '2024-07-20', 1198.00, 'Completed'),
('ORD021', 'CUST013', '2024-08-01', 2347.00, 'Completed'),
('ORD022', 'CUST002', '2024-08-10', 567.00, 'Completed'),
('ORD023', 'CUST014', '2024-08-15', 234.00, 'Processing'),
('ORD024', 'CUST015', '2024-08-20', 1456.00, 'Shipped'),
('ORD025', 'CUST003', '2024-08-25', 893.00, 'Processing');

-- ===================================================================
-- TABLE 8: ORDER_ITEMS
-- ===================================================================
CREATE TABLE order_items (
    order_item_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INT,
    price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

INSERT INTO order_items VALUES
-- ORD001 items
('OI001', 'ORD001', 'PROD001', 1, 999.00),
('OI002', 'ORD001', 'PROD004', 1, 399.00),
('OI003', 'ORD001', 'PROD006', 1, 129.00),
('OI004', 'ORD001', 'PROD008', 2, 35.00),

-- ORD002 items
('OI005', 'ORD002', 'PROD006', 2, 129.00),

-- ORD003 items
('OI006', 'ORD003', 'PROD002', 1, 1199.00),
('OI007', 'ORD003', 'PROD003', 1, 699.00),

-- ORD004 items
('OI008', 'ORD004', 'PROD004', 1, 399.00),
('OI009', 'ORD004', 'PROD008', 1, 35.00),

-- ORD005 items
('OI010', 'ORD005', 'PROD011', 1, 89.00),
('OI011', 'ORD005', 'PROD013', 1, 89.00),

-- ORD006 items
('OI012', 'ORD006', 'PROD003', 1, 699.00),
('OI013', 'ORD006', 'PROD016', 2, 15.00),

-- ORD007 items
('OI014', 'ORD007', 'PROD007', 2, 89.00),
('OI015', 'ORD007', 'PROD008', 4, 35.00),
('OI016', 'ORD007', 'PROD010', 1, 45.00),

-- ORD008 items
('OI017', 'ORD008', 'PROD002', 1, 1199.00),
('OI018', 'ORD008', 'PROD011', 1, 299.00),
('OI019', 'ORD008', 'PROD021', 1, 35.00),
('OI020', 'ORD008', 'PROD016', 1, 15.00),

-- ORD009 items
('OI021', 'ORD009', 'PROD012', 1, 649.00),
('OI022', 'ORD009', 'PROD015', 1, 119.00),

-- ORD010 items
('OI023', 'ORD010', 'PROD006', 2, 129.00),
('OI024', 'ORD010', 'PROD021', 1, 35.00),

-- ORD011 items
('OI025', 'ORD011', 'PROD002', 1, 1199.00),
('OI026', 'ORD011', 'PROD014', 1, 79.00),

-- ORD012 items
('OI027', 'ORD012', 'PROD007', 1, 89.00),
('OI028', 'ORD012', 'PROD008', 2, 35.00),

-- ORD013 items
('OI029', 'ORD013', 'PROD004', 1, 399.00),
('OI030', 'ORD013', 'PROD021', 1, 35.00),
('OI031', 'ORD013', 'PROD016', 1, 15.00),

-- ORD014 items
('OI032', 'ORD014', 'PROD002', 1, 1199.00),
('OI033', 'ORD014', 'PROD001', 1, 999.00),

-- ORD015 items
('OI034', 'ORD015', 'PROD012', 1, 649.00),
('OI035', 'ORD015', 'PROD022', 2, 89.00),

-- More order items
('OI036', 'ORD016', 'PROD004', 1, 399.00),
('OI037', 'ORD017', 'PROD002', 1, 1199.00),
('OI038', 'ORD017', 'PROD004', 1, 399.00),
('OI039', 'ORD018', 'PROD003', 1, 699.00),
('OI040', 'ORD018', 'PROD006', 2, 129.00),
('OI041', 'ORD019', 'PROD007', 2, 89.00),
('OI042', 'ORD019', 'PROD011', 1, 89.00),
('OI043', 'ORD020', 'PROD002', 1, 1199.00),
('OI044', 'ORD021', 'PROD001', 2, 999.00),
('OI045', 'ORD021', 'PROD011', 1, 299.00),
('OI046', 'ORD022', 'PROD004', 1, 399.00),
('OI047', 'ORD022', 'PROD006', 1, 129.00),
('OI048', 'ORD023', 'PROD007', 2, 89.00),
('OI049', 'ORD024', 'PROD002', 1, 1199.00),
('OI050', 'ORD025', 'PROD003', 1, 699.00);

-- ===================================================================
-- VERIFY DATA
-- ===================================================================

-- Show counts
SELECT 'Customers' as table_name, COUNT(*) as row_count FROM customers
UNION ALL
SELECT 'Transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'Accounts', COUNT(*) FROM accounts
UNION ALL
SELECT 'Employees', COUNT(*) FROM employees
UNION ALL
SELECT 'Product Categories', COUNT(*) FROM product_categories
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Items', COUNT(*) FROM order_items;

-- ===================================================================
-- USEFUL QUERIES FOR TESTING
-- ===================================================================

-- Check transaction date range
SELECT 
    MIN(transaction_date) as earliest_txn,
    MAX(transaction_date) as latest_txn,
    COUNT(DISTINCT customer_id) as unique_customers
FROM transactions;

-- Check customer with most transactions
SELECT 
    customer_id,
    COUNT(*) as txn_count,
    SUM(amount) as total_spent
FROM transactions
GROUP BY customer_id
ORDER BY txn_count DESC
LIMIT 10;

-- Check employee hierarchy
SELECT 
    e.name as employee,
    m.name as manager,
    e.department,
    e.salary
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.employee_id
ORDER BY e.salary DESC;

COMMIT;

-- ===================================================================
-- DATABASE SETUP COMPLETE!
-- ===================================================================
-- You can now practice all SQL questions from the interview guide
-- 
-- Sample queries to get started:
-- 
-- 1. SELECT * FROM customers LIMIT 10;
-- 2. SELECT * FROM transactions WHERE amount > 10000;
-- 3. SELECT customer_id, COUNT(*) FROM transactions GROUP BY customer_id;
-- 
-- Good luck with your practice! 🚀
-- ===================================================================

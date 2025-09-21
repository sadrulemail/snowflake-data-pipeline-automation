CREATE DATABASE iceberg_demo_db
LOCATION 's3://iceberg-s3-bucket-demo-3486386/tables-data/';

drop TABLE iceberg_demo_db.manufacturers;

-- Create manufacturers table with Iceberg format
CREATE TABLE iceberg_demo_db.manufacturers (
    manufacturer_id BIGINT,
    manufacturer_name STRING,
    country STRING,
    contact_email STRING,
    phone STRING,
    is_active BOOLEAN
)
LOCATION 's3://iceberg-s3-bucket-demo-3486386/tables-data/manufacturers'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression'='snappy'
);

-- Insert manufacturer data
INSERT INTO iceberg_demo_db.manufacturers VALUES
(1001, 'MediSafe Supplies', 'USA', 'orders@medisafe.com', '+1-800-555-1001', true),
(1002, 'EuroHealth GmbH', 'Germany', 'info@eurohealth.de', '+49 30 12345678', true),
(1003, 'BioScan Inc.', 'Japan', 'sales@bioscan.jp', '+81 3 1234 5678', true);

INSERT INTO iceberg_demo_db.manufacturers VALUES
(1004, 'PharmaCare Ltd.', 'UK', 'contact@pharmacare.co.uk', '+44 20 7946 0958', true),
(1005, 'MediTech Solutions', 'Canada', 'info@meditech.ca', '+1-416-555-1234', true),
(1006, 'Global Medical Devices', 'USA', 'sales@globalmed.com', '+1-800-555-6789', true),
(1007, 'AsiaHealth Corp.', 'China', 'support@asiahealth.cn', '+86 10 8520 1234', true),
(1008, 'Nordic Medical AB', 'Sweden', 'orders@nordicmedical.se', '+46 8 123 4567', true),
(1009, 'AusMed Pty Ltd', 'Australia', 'info@ausmed.com.au', '+61 2 9876 5432', true),
(1010, 'LatinMed SA', 'Brazil', 'contato@latinmed.com.br', '+55 11 3456 7890', false);



-- Create products table with Iceberg format
CREATE TABLE iceberg_demo_db.products (
    product_id BIGINT,
    product_name STRING,
    manufacturer_id BIGINT,
    category STRING,
    unit_cost DOUBLE,
    unit_price DOUBLE,
    sku STRING,
    is_active BOOLEAN
)
LOCATION 's3://iceberg-s3-bucket-demo-3486386/tables-data/products'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression'='snappy'
);

-- Insert product data
INSERT INTO iceberg_demo_db.products VALUES
(2001, 'N95 Respirator Mask (50pk)', 1001, 'PPE', 12.50, 29.99, 'MSK-N95-50', true),
(2002, 'Nitrile Gloves (Box of 100)', 1001, 'PPE', 8.75, 19.95, 'GLV-NIT-100', true),
(2003, 'Digital Thermometer Pro', 1003, 'Monitoring', 15.00, 34.50, 'THERM-DIG-PRO', true);

-- Insert more products across different categories
INSERT INTO iceberg_demo_db.products VALUES
(2004, 'Disposable Surgical Gowns', 1001, 'PPE', 6.25, 14.99, 'GOWN-DSP-1', true),
(2005, 'Portable Oxygen Concentrator', 1003, 'Therapy', 285.00, 599.99, 'OXY-CON-PORT', true),
(2006, 'Blood Pressure Monitor', 1003, 'Monitoring', 22.50, 49.95, 'BP-MON-DIG', true),
(2007, 'Sterile Syringes (100pk)', 1002, 'Disposables', 4.80, 11.99, 'SYR-STR-100', true),
(2008, 'Medical Face Shields (25pk)', 1004, 'PPE', 18.00, 39.95, 'FSH-MED-25', true),
(2009, 'Electronic Stethoscope', 1006, 'Diagnostics', 45.00, 99.50, 'STETH-ELEC', true),
(2010, 'Wheelchair Transport', 1005, 'Mobility', 120.00, 249.99, 'WCHR-TRANS', true),
(2011, 'First Aid Kit Deluxe', 1004, 'Emergency', 15.75, 34.99, 'FAK-DELUXE', true),
(2012, 'Hospital Bed Adjustable', 1002, 'Equipment', 450.00, 899.99, 'BED-ADJ-HOSP', true),
(2013, 'Insulin Cooler Case', 1007, 'Storage', 8.90, 19.95, 'INSULIN-COOL', false);


-- Create inventory table with Iceberg format
CREATE TABLE iceberg_demo_db.inventory (
    inventory_id BIGINT,
    product_id BIGINT,
    quantity_on_hand INT,
    low_stock_threshold INT,
    last_restocked_date DATE,
    warehouse_location STRING
)
LOCATION 's3://iceberg-s3-bucket-demo-3486386/tables-data/inventory'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression'='snappy'
);

-- Insert inventory data
INSERT INTO iceberg_demo_db.inventory VALUES
(1, 2001, 1500, 100, DATE '2023-10-15', 'Warehouse A'),
(2, 2002, 850, 75, DATE '2023-10-18', 'Warehouse A'),
(3, 2003, 420, 30, DATE '2023-10-10', 'Warehouse B'),
(4, 2004, 210, 25, DATE '2023-10-05', 'Warehouse B'),
(5, 2005, 1200, 50, DATE '2023-10-20', 'Warehouse C');

-- Insert more inventory records
INSERT INTO iceberg_demo_db.inventory VALUES
(6, 2006, 350, 40, DATE '2023-10-12', 'Warehouse C'),
(7, 2007, 1800, 200, DATE '2023-10-22', 'Warehouse A'),
(8, 2008, 95, 50, DATE '2023-10-08', 'Warehouse B'),
(9, 2009, 220, 30, DATE '2023-10-14', 'Warehouse C'),
(10, 2010, 75, 25, DATE '2023-10-16', 'Warehouse A'),
(11, 2011, 480, 100, DATE '2023-10-19', 'Warehouse B'),
(12, 2012, 35, 10, DATE '2023-10-25', 'Warehouse C'),
(13, 2013, 0, 15, DATE '2023-09-30', 'Warehouse A');

-- Create customers table with Iceberg format
CREATE TABLE iceberg_demo_db.customers (
    customer_id BIGINT,
    customer_name STRING,
    type STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    is_active BOOLEAN
)
LOCATION 's3://iceberg-s3-bucket-demo-3486386/tables-data/customers'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression'='snappy'
);

-- Insert customer data
INSERT INTO iceberg_demo_db.customers VALUES
(3001, 'City General Hospital', 'Hospital', 'procurement@citygeneral.org', '+1-555-1234', '123 Medical Way', 'Boston', 'MA', '02110', true),
(3002, 'Wellness Pharmacy', 'Pharmacy', 'orders@wellnesspharmacy.com', '+1-555-5678', '456 Health Ave', 'Chicago', 'IL', '60601', true),
(3003, 'Community Health Clinic', 'Clinic', 'supplies@communityclinic.org', '+1-555-9012', '789 Care Street', 'Austin', 'TX', '73301', true),
(3004, 'MedFirst Partners', 'Distributor', 'account@medfirst.com', '+1-555-3456', '321 Distribution Blvd', 'Denver', 'CO', '80202', true),
(3005, 'Regional Medical Center', 'Hospital', 'admin@regionalmedical.org', '+1-555-7890', '555 Hospital Drive', 'Atlanta', 'GA', '30301', true),
(3006, 'University Hospital', 'Hospital', 'supply@universityhospital.edu', '+1-555-2345', '100 Campus Road', 'Los Angeles', 'CA', '90001', true);

-- Insert more customers across different types
INSERT INTO iceberg_demo_db.customers VALUES
(3007, 'Neighborhood Drugstore', 'Pharmacy', 'contact@neighborhooddrug.com', '+1-555-6789', '222 Main Street', 'Seattle', 'WA', '98101', true),
(3008, 'Specialty Surgical Center', 'Clinic', 'admin@specialtysurgical.com', '+1-555-0123', '333 Surgery Lane', 'Miami', 'FL', '33101', true),
(3009, 'Global Medical Distributors', 'Distributor', 'sales@globalmeddist.com', '+1-555-4567', '444 Trade Center', 'Phoenix', 'AZ', '85001', true),
(3010, 'Childrens Medical Institute', 'Hospital', 'procure@childrensmed.org', '+1-555-8901', '555 Pediatric Drive', 'Philadelphia', 'PA', '19101', true),
(3011, 'Senior Care Pharmacy', 'Pharmacy', 'orders@seniorcarepharm.com', '+1-555-2346', '666 Elder Care Road', 'San Francisco', 'CA', '94101', true),
(3012, 'Urgent Care Plus', 'Clinic', 'supplies@urgentcareplus.com', '+1-555-6780', '777 Emergency Way', 'Dallas', 'TX', '75201', true),
(3013, 'HealthPlus Distributors', 'Distributor', 'info@healthplusdist.com', '+1-555-9013', '888 Supply Chain Ave', 'Detroit', 'MI', '48201', false);


-- Create sales table with Iceberg format
CREATE TABLE iceberg_demo_db.sales (
    sale_id BIGINT,
    customer_id BIGINT,
    sale_date DATE,
    total_amount DOUBLE,
    status STRING,
    payment_method STRING,
    invoice_number STRING
)
LOCATION 's3://iceberg-s3-bucket-demo-3486386/tables-data/sales'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression'='snappy'
);

-- Insert sales data
INSERT INTO iceberg_demo_db.sales VALUES
(5001, 3001, DATE '2023-10-01', 2549.40, 'Shipped', 'Credit Card', 'INV-2023-1001'),
(5002, 3002, DATE '2023-10-01', 1247.50, 'Processing', 'Bank Transfer', 'INV-2023-1002'),
(5003, 3003, DATE '2023-10-02', 789.95, 'Completed', 'Credit Card', 'INV-2023-1003');

-- Insert more sales records across different dates and customers
INSERT INTO iceberg_demo_db.sales VALUES
(5004, 3004, DATE '2023-10-02', 3456.75, 'Completed', 'Purchase Order', 'INV-2023-1004'),
(5005, 3005, DATE '2023-10-03', 5123.90, 'Shipped', 'Credit Card', 'INV-2023-1005'),
(5006, 3001, DATE '2023-10-03', 1875.25, 'Processing', 'Bank Transfer', 'INV-2023-1006'),
(5007, 3006, DATE '2023-10-04', 2987.60, 'Completed', 'Credit Card', 'INV-2023-1007'),
(5008, 3002, DATE '2023-10-04', 956.80, 'Shipped', 'Credit Card', 'INV-2023-1008'),
(5009, 3007, DATE '2023-10-05', 4231.45, 'Processing', 'Purchase Order', 'INV-2023-1009'),
(5010, 3005, DATE '2023-10-05', 1678.30, 'Completed', 'Bank Transfer', 'INV-2023-1010'),
(5011, 3008, DATE '2023-10-06', 2890.15, 'Shipped', 'Credit Card', 'INV-2023-1011'),
(5012, 3003, DATE '2023-10-06', 1345.90, 'Processing', 'Credit Card', 'INV-2023-1012'),
(5013, 3009, DATE '2023-10-07', 4765.20, 'Completed', 'Purchase Order', 'INV-2023-1013');


-- Create sales_line_items table with Iceberg format
CREATE TABLE iceberg_demo_db.sales_line_items (
    sale_item_id BIGINT,
    sale_id BIGINT,
    product_id BIGINT,
    quantity INT,
    unit_price DOUBLE
)
LOCATION 's3://iceberg-s3-bucket-demo-3486386/tables-data/sales_line_items'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'PARQUET',
    'write_compression'='snappy'
);

-- Insert sales line items data
INSERT INTO iceberg_demo_db.sales_line_items VALUES
(1, 5001, 2001, 50, 29.99),
(2, 5001, 2002, 30, 19.95),
(3, 5001, 2007, 200, 1.25);

-- Insert more sales line items
INSERT INTO iceberg_demo_db.sales_line_items VALUES
(4, 5002, 2002, 25, 19.95),
(5, 5002, 2006, 8, 49.95),
(6, 5002, 2011, 15, 34.99),
(7, 5003, 2003, 12, 34.50),
(8, 5003, 2004, 40, 14.99),
(9, 5004, 2005, 3, 599.99),
(10, 5004, 2009, 5, 99.50),
(11, 5005, 2001, 100, 29.99),
(12, 5005, 2008, 50, 39.95),
(13, 5006, 2002, 60, 19.95),
(14, 5006, 2007, 300, 1.25),
(15, 5007, 2010, 2, 249.99),
(16, 5007, 2012, 1, 899.99);


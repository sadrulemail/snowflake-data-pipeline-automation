create schema if not exists TRANSFORM_DATA;

use schema TRANSFORM_DATA;


CREATE OR REPLACE TABLE DIM_MANUFACTURERS (
    manufacturer_key NUMBER PRIMARY KEY AUTOINCREMENT,
    manufacturer_id NUMBER,
    manufacturer_name VARCHAR NOT NULL,
    country VARCHAR,
    contact_email VARCHAR,
    phone VARCHAR,
    is_active BOOLEAN,
    valid_from_date DATE,
    valid_to_date DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE DIM_PRODUCTS (
    product_key NUMBER PRIMARY KEY AUTOINCREMENT,
    product_id NUMBER,
    manufacturer_id NUMBER NOT NULL,
    product_name VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    unit_cost FLOAT NOT NULL,
    unit_price FLOAT NOT NULL,
    sku VARCHAR,
    is_active BOOLEAN,
    valid_from_date DATE,
    valid_to_date DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE DIM_INVENTORY (
    inventory_key NUMBER PRIMARY KEY AUTOINCREMENT,
    inventory_id NUMBER,
    product_id NUMBER NOT NULL,
    quantity_on_hand NUMBER NOT NULL,
    low_stock_threshold NUMBER NOT NULL,
    last_restocked_date DATE,
    warehouse_location VARCHAR NOT NULL,
    valid_from_date DATE,
    valid_to_date DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    customer_key NUMBER PRIMARY KEY AUTOINCREMENT,
    customer_id NUMBER,
    customer_name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    email VARCHAR,
    phone VARCHAR,
    address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip_code VARCHAR,
    is_active BOOLEAN,
    valid_from_date DATE,
    valid_to_date DATE,
    is_current BOOLEAN
);


--fact

CREATE OR REPLACE TABLE FACT_SALES (
    sale_id NUMBER PRIMARY KEY,
    customer_key NUMBER NOT NULL,
    sale_date DATE NOT NULL,
    total_amount FLOAT NOT NULL,
    status VARCHAR NOT NULL,
    payment_method VARCHAR NOT NULL,
    invoice_number VARCHAR UNIQUE NOT NULL,
    FOREIGN KEY (customer_key) REFERENCES DIM_CUSTOMERS(customer_key)
);

CREATE OR REPLACE TABLE FACT_SALES_LINE_ITEMS (
    sale_item_id NUMBER PRIMARY KEY,
    sale_id NUMBER NOT NULL,
    product_key NUMBER NOT NULL,
    quantity NUMBER NOT NULL,
    unit_price FLOAT NOT NULL,
    line_total_amount FLOAT NOT NULL,
    FOREIGN KEY (sale_id) REFERENCES FACT_SALES(sale_id),
    FOREIGN KEY (product_key) REFERENCES DIM_PRODUCTS(product_key)
);

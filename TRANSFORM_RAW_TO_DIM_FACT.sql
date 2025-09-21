
/*
CALL common_sch.TRANSFORM_RAW_TO_DIM_FACT('RAW_DATA', 'TRANSFORM_DATA', 'ALL');

CALL common_sch.TRANSFORM_RAW_TO_DIM_FACT('RAW_DATA', 'TRANSFORM_DATA', 'DIM_CUSTOMERS');

*/

-- drop PROCEDURE TRANSFORM_DATA.TRANSFORM_RAW_TO_DIM_FACT(string,string, string);


CREATE OR REPLACE PROCEDURE common_sch.TRANSFORM_RAW_TO_DIM_FACT(
    RAW_SCH_NAME VARCHAR,
    TRANSFORM_SCH_NAME VARCHAR,
    TABLE_TYPE VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
try {
    var rawSchemaName = RAW_SCH_NAME;
    var transformSchemaName = TRANSFORM_SCH_NAME;
    var tableType = TABLE_TYPE.toUpperCase(); // Normalize input
    var resultMessage = "";
    var sqlText = '';

    switch (tableType) {
        case 'DIM_CUSTOMERS':
        case 'DIM_MANUFACTURERS':
        case 'DIM_PRODUCTS':
        case 'DIM_INVENTORY':
        case 'FACT_SALES':
        case 'FACT_SALES_LINE_ITEMS':
        case 'ALL':
            break; // Valid option, proceed
        default:
            throw "Invalid TABLE_TYPE. Valid values: DIM_CUSTOMERS, DIM_MANUFACTURERS, DIM_PRODUCTS, DIM_INVENTORY, FACT_SALES, FACT_SALES_LINE_ITEMS, ALL";
    }
    
    // Logic for ALL tables (includes all steps below in correct order)
    if (tableType === 'ALL' || tableType === 'DIM_CUSTOMERS') {
        sqlText = `
            MERGE INTO ${transformSchemaName}.DIM_CUSTOMERS T
            USING (
                SELECT
                    customer_id,
                    customer_name,
                    type,
                    email,
                    phone,
                    address,
                    city,
                    state,
                    zip_code,
                    is_active
                FROM ${rawSchemaName}.customers
            ) S
            ON T.customer_id = S.customer_id AND T.is_current = TRUE
            WHEN MATCHED AND (
                T.customer_name <> S.customer_name OR T.type <> S.type OR T.email <> S.email OR T.phone <> S.phone OR T.address <> S.address OR T.city <> S.city OR T.state <> S.state OR T.zip_code <> S.zip_code OR T.is_active <> S.is_active
            ) THEN
                UPDATE SET is_current = FALSE, valid_to_date = CURRENT_DATE() - 1
            WHEN NOT MATCHED THEN
                INSERT (
                    customer_id, customer_name, type, email, phone, address, city, state, zip_code, is_active, valid_from_date, valid_to_date, is_current
                ) VALUES (
                    S.customer_id, S.customer_name, S.type, S.email, S.phone, S.address, S.city, S.state, S.zip_code, S.is_active, CURRENT_DATE(), NULL, TRUE
                );
        `;
        snowflake.createStatement({sqlText: sqlText}).execute();
        resultMessage += "DIM_CUSTOMERS loaded successfully. ";
    }

    if (tableType === 'ALL' || tableType === 'DIM_MANUFACTURERS') {
        sqlText = `
            MERGE INTO ${transformSchemaName}.DIM_MANUFACTURERS T
            USING (
                SELECT
                    manufacturer_id,
                    manufacturer_name,
                    country,
                    contact_email,
                    phone,
                    is_active
                FROM ${rawSchemaName}.manufacturers
            ) S
            ON T.manufacturer_id = S.manufacturer_id AND T.is_current = TRUE
            WHEN MATCHED AND (
                T.manufacturer_name <> S.manufacturer_name OR T.country <> S.country OR T.contact_email <> S.contact_email OR T.phone <> S.phone OR T.is_active <> S.is_active
            ) THEN
                UPDATE SET is_current = FALSE, valid_to_date = CURRENT_DATE() - 1
            WHEN NOT MATCHED THEN
                INSERT (
                    manufacturer_id, manufacturer_name, country, contact_email, phone, is_active, valid_from_date, valid_to_date, is_current
                ) VALUES (
                    S.manufacturer_id, S.manufacturer_name, S.country, S.contact_email, S.phone, S.is_active, CURRENT_DATE(), NULL, TRUE
                );
        `;
        snowflake.createStatement({sqlText: sqlText}).execute();
        resultMessage += "DIM_MANUFACTURERS loaded successfully. ";
    }

    if (tableType === 'ALL' || tableType === 'DIM_PRODUCTS') {
        sqlText = `
            MERGE INTO ${transformSchemaName}.DIM_PRODUCTS T
            USING (
                SELECT
                    product_id,
                    manufacturer_id,
                    product_name,
                    category,
                    unit_cost,
                    unit_price,
                    sku,
                    is_active
                FROM ${rawSchemaName}.products
            ) S
            ON T.product_id = S.product_id AND T.is_current = TRUE
            WHEN MATCHED AND (
                T.manufacturer_id <> S.manufacturer_id OR T.product_name <> S.product_name OR T.category <> S.category OR T.unit_cost <> S.unit_cost OR T.unit_price <> S.unit_price OR T.sku <> S.sku OR T.is_active <> S.is_active
            ) THEN
                UPDATE SET is_current = FALSE, valid_to_date = CURRENT_DATE() - 1
            WHEN NOT MATCHED THEN
                INSERT (
                    product_id, manufacturer_id, product_name, category, unit_cost, unit_price, sku, is_active, valid_from_date, valid_to_date, is_current
                ) VALUES (
                    S.product_id, S.manufacturer_id, S.product_name, S.category, S.unit_cost, S.unit_price, S.sku, S.is_active, CURRENT_DATE(), NULL, TRUE
                );
        `;
        snowflake.createStatement({sqlText: sqlText}).execute();
        resultMessage += "DIM_PRODUCTS loaded successfully. ";
    }

    if (tableType === 'ALL' || tableType === 'DIM_INVENTORY') {
        sqlText = `
            MERGE INTO ${transformSchemaName}.DIM_INVENTORY T
            USING (
                SELECT
                    inventory_id,
                    product_id,
                    quantity_on_hand,
                    low_stock_threshold,
                    last_restocked_date,
                    warehouse_location
                FROM ${rawSchemaName}.inventory
            ) S
            ON T.inventory_id = S.inventory_id AND T.is_current = TRUE
            WHEN MATCHED AND (
                T.product_id <> S.product_id OR T.quantity_on_hand <> S.quantity_on_hand OR T.low_stock_threshold <> S.low_stock_threshold OR T.last_restocked_date <> S.last_restocked_date OR T.warehouse_location <> S.warehouse_location
            ) THEN
                UPDATE SET is_current = FALSE, valid_to_date = CURRENT_DATE() - 1
            WHEN NOT MATCHED THEN
                INSERT (
                    inventory_id, product_id, quantity_on_hand, low_stock_threshold, last_restocked_date, warehouse_location, valid_from_date, valid_to_date, is_current
                ) VALUES (
                    S.inventory_id, S.product_id, S.quantity_on_hand, S.low_stock_threshold, S.last_restocked_date, S.warehouse_location, CURRENT_DATE(), NULL, TRUE
                );
        `;
        snowflake.createStatement({sqlText: sqlText}).execute();
        resultMessage += "DIM_INVENTORY loaded successfully. ";
    }

    if (tableType === 'ALL' || tableType === 'FACT_SALES') {
        sqlText = `
            MERGE INTO ${transformSchemaName}.FACT_SALES T
            USING (
                SELECT
                    s.sale_id,
                    c.customer_key,
                    s.sale_date,
                    s.total_amount,
                    s.status,
                    s.payment_method,
                    s.invoice_number
                FROM ${rawSchemaName}.sales s
                JOIN ${transformSchemaName}.DIM_CUSTOMERS c ON s.customer_id = c.customer_id AND c.is_current = TRUE
            ) S
            ON T.sale_id = S.sale_id
            WHEN MATCHED THEN
                UPDATE SET T.customer_key = S.customer_key, T.sale_date = S.sale_date, T.total_amount = S.total_amount, T.status = S.status, T.payment_method = S.payment_method, T.invoice_number = S.invoice_number
            WHEN NOT MATCHED THEN
                INSERT (
                    sale_id, customer_key, sale_date, total_amount, status, payment_method, invoice_number
                ) VALUES (
                    S.sale_id, S.customer_key, S.sale_date, S.total_amount, S.status, S.payment_method, S.invoice_number
                );
        `;
        snowflake.createStatement({sqlText: sqlText}).execute();
        resultMessage += "FACT_SALES merged successfully. ";
    }
    
    if (tableType === 'ALL' || tableType === 'FACT_SALES_LINE_ITEMS') {
        sqlText = `
            MERGE INTO ${transformSchemaName}.FACT_SALES_LINE_ITEMS T
            USING (
                SELECT
                    sli.sale_item_id,
                    sli.sale_id,
                    p.product_key,
                    sli.quantity,
                    sli.unit_price,
                    (sli.quantity * sli.unit_price) as line_total_amount
                FROM ${rawSchemaName}.sales_line_items sli
                JOIN ${transformSchemaName}.DIM_PRODUCTS p ON sli.product_id = p.product_id AND p.is_current = TRUE
            ) S
            ON T.sale_item_id = S.sale_item_id
            WHEN MATCHED THEN
                UPDATE SET T.sale_id = S.sale_id, T.product_key = S.product_key, T.quantity = S.quantity, T.unit_price = S.unit_price, T.line_total_amount = S.line_total_amount
            WHEN NOT MATCHED THEN
                INSERT (
                    sale_item_id, sale_id, product_key, quantity, unit_price, line_total_amount
                ) VALUES (
                    S.sale_item_id, S.sale_id, S.product_key, S.quantity, S.unit_price, S.line_total_amount
                );
        `;
        snowflake.createStatement({sqlText: sqlText}).execute();
        resultMessage += "FACT_SALES_LINE_ITEMS merged successfully. ";
    }
    
    return "All transformations completed. " + resultMessage;
} catch (err) {
    return "Error: " + err.message + "\nStatement that failed:\n" + sqlText;
}
$$;
/*
Purpose: native table is faster compare to iceberg table

CALL common_sch.load_data_iceberg_to_snowflake_native('LANDING_DATA', 'RAW_DATA', 'ALL');
CALL common_sch.load_data_iceberg_to_snowflake_native('LANDING_DATA', 'RAW_DATA', 'PRODUCTS');
*/

CREATE OR REPLACE PROCEDURE common_sch.load_data_iceberg_to_snowflake_native(
    ICEBERG_SCHEMA_NAME VARCHAR,
    RAW_SCHEMA_NAME VARCHAR,
    TABLE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
try {
    var icebergSchemaName = ICEBERG_SCHEMA_NAME;
    var snowflakeRawSchemaName = RAW_SCHEMA_NAME;
    var tableName = TABLE_NAME.toUpperCase();
    var statement_print = '';
    var tablesToLoad = [];

    // Centralized table definitions for Iceberg and native tables
    var tableDefinitions = {
        'MANUFACTURERS': {
            iceberg: 'manufacturers',
            native: 'manufacturers',
            columns: 'manufacturer_id, manufacturer_name, country, contact_email, phone, is_active'
        },
        'PRODUCTS': {
            iceberg: 'products',
            native: 'products',
            columns: 'product_id, product_name, manufacturer_id, category, unit_cost, unit_price, sku, is_active'
        },
        'INVENTORY': {
            iceberg: 'inventory',
            native: 'inventory',
            columns: 'inventory_id, product_id, quantity_on_hand, low_stock_threshold, last_restocked_date, warehouse_location'
        },
        'CUSTOMERS': {
            iceberg: 'customers',
            native: 'customers',
            columns: 'customer_id, customer_name, type, email, phone, address, city, state, zip_code, is_active'
        },
        'SALES': {
            iceberg: 'sales',
            native: 'sales',
            columns: 'sale_id, customer_id, sale_date, total_amount, status, payment_method, invoice_number'
        },
        'SALES_LINE_ITEMS': {
            iceberg: 'sales_line_items',
            native: 'sales_line_items',
            columns: 'sale_item_id, sale_id, product_id, quantity, unit_price'
        }
    };

    if (tableName === 'ALL') {
        for (var key in tableDefinitions) {
            tablesToLoad.push(tableDefinitions[key]);
        }
    } else if (tableDefinitions[tableName]) {
        tablesToLoad.push(tableDefinitions[tableName]);
    } else {
        throw "Invalid table name. Valid values: MANUFACTURERS, PRODUCTS, INVENTORY, CUSTOMERS, SALES, SALES_LINE_ITEMS, or ALL.";
    }

    var resultMessage = "";

    // Loop through the selected tables and load all columns
    for (var i = 0; i < tablesToLoad.length; i++) {
        var table = tablesToLoad[i];
        
        statement_print = `
            INSERT INTO ${snowflakeRawSchemaName}.${table.native} (${table.columns})
            SELECT ${table.columns} FROM ${icebergSchemaName}.${table.iceberg};
        `;

        var stmt = snowflake.createStatement({sqlText: statement_print});
        var result = stmt.execute();
        resultMessage += `Successfully loaded data from ${icebergSchemaName}.${table.iceberg} to ${snowflakeRawSchemaName}.${table.native}. `;
    }

    return `Operation complete. ${resultMessage}`;

} catch (err) {
    return `Error: ${err.message} \nSQL attempted: ${statement_print.trim()}`;
}
$$;
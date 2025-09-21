/*
-- Creates all six tables
CALL common_sch.create_iceberg_tables('LANDING_DATA', 'glueCatalog_extvol', 'glueCatalogInt');

-- Also creates all six tables
CALL common_sch.create_iceberg_tables('LANDING_DATA', 'glueCatalog_extvol', 'glueCatalogInt', 'ALL');

-- Creates only the 'products' table
CALL common_sch.create_iceberg_tables('LANDING_DATA', 'glueCatalog_extvol', 'glueCatalogInt', 'products');

*/

create schema if not exists LANDING_DATA;


CREATE OR REPLACE PROCEDURE common_sch.create_iceberg_tables(
    schema_name VARCHAR,
    external_volume VARCHAR,
    catalog_name VARCHAR,
    table_to_create VARCHAR DEFAULT 'ALL'
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var all_tables = [
    "manufacturers",
    "products",
    "customers",
    "inventory",
    "sales",
    "sales_line_items"
  ];
  
  var tables_to_process;
  var results = [];
  
  // Check if a specific table was passed, otherwise process all tables.
  if (TABLE_TO_CREATE && TABLE_TO_CREATE.toUpperCase() !== 'ALL') {
    tables_to_process = [TABLE_TO_CREATE];
  } else {
    tables_to_process = all_tables;
  }
  
  // Iterate through the selected tables
  for (var i = 0; i < tables_to_process.length; i++) {
    var tableName = tables_to_process[i];
    
    // Construct the SQL query with the passed parameters
    var createTableSQL = `
      CREATE OR REPLACE ICEBERG TABLE ${SCHEMA_NAME}.${tableName}
      EXTERNAL_VOLUME='${EXTERNAL_VOLUME}'
      CATALOG='${CATALOG_NAME}'
      CATALOG_TABLE_NAME='${tableName}'
      AUTO_REFRESH = TRUE;
    `;
    
    try {
      snowflake.execute({sqlText: createTableSQL});
      results.push(`Successfully created table: ${SCHEMA_NAME}.${tableName}`);
    } catch (err) {
      results.push(`Failed to create table ${SCHEMA_NAME}.${tableName}: ${err.message}`);
    }
  }

  return results.join('\n');
$$;
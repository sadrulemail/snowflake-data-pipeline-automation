/*
-- Refreshes all six tables
CALL common_sch.refresh_iceberg_tables('LANDING_DATA', 'ALL');

-- Refreshes only the 'products' table
CALL common_sch.refresh_iceberg_tables('LANDING_DATA', 'products');

*/

CREATE OR REPLACE PROCEDURE common_sch.refresh_iceberg_tables(
    schema_name VARCHAR,
    table_to_refresh VARCHAR DEFAULT 'ALL'
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
  if (TABLE_TO_REFRESH && TABLE_TO_REFRESH.toUpperCase() !== 'ALL') {
    // Validate that the specified table is in the list of known tables
    if (all_tables.indexOf(TABLE_TO_REFRESH.toLowerCase()) > -1) {
      tables_to_process = [TABLE_TO_REFRESH];
    } else {
      return `Error: Table '${TABLE_TO_REFRESH}' is not a valid table name to refresh.`;
    }
  } else {
    tables_to_process = all_tables;
  }
  
  // Iterate through the selected tables and execute the refresh command
  for (var i = 0; i < tables_to_process.length; i++) {
    var tableName = tables_to_process[i];
    
    // Construct the SQL query
    var refreshTableSQL = `ALTER ICEBERG TABLE ${SCHEMA_NAME}.${tableName} REFRESH;`;
    
    try {
      snowflake.execute({sqlText: refreshTableSQL});
      results.push(`Successfully refreshed table: ${SCHEMA_NAME}.${tableName}`);
    } catch (err) {
      results.push(`Failed to refresh table ${SCHEMA_NAME}.${tableName}: ${err.message}`);
    }
  }

  return results.join('\n');
$$;
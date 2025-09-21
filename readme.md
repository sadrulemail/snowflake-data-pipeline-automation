Snowflake Data Pipeline Automation
This repository contains an interactive web application that serves as a guide to building and automating a data pipeline using Snowflake, AWS S3, and Apache Iceberg. The application visualizes the step-by-step process, from sourcing raw data to transforming it and generating business insights.

Project Overview
The core purpose of this project is to demonstrate an end-to-end data pipeline solution. It begins with raw data in an AWS S3 data lake, leverages Iceberg tables for a flexible schema, and performs transformations within Snowflake to create a clean, dimensional data model. The final stage presents business intelligence queries with visualizations.

Core Technologies
Snowflake: The primary cloud data platform for data loading, storage, transformation, and analysis.

AWS S3: The cloud storage service used as the data lake and a source for external tables.

AWS Athena & Glue: Services used for cataloging S3 data and defining the initial Iceberg tables.

Apache Iceberg: An open table format that provides a flexible, performant way to manage external data.

Pipeline Steps
The interactive guide walks through the following stages of the data pipeline:

1. Source Data (Script: iceberg_tbl_def_and_data_aws_s3.sql)
The pipeline begins with data stored in an AWS S3 bucket. AWS Glue catalogs this data, and AWS Athena is used to define and populate the initial Iceberg tables. This establishes the single source of truth for our Snowflake pipeline, from which all subsequent steps are based.

2. Create Iceberg Tables (Procedure: create_iceberg_tables)
This procedure creates Iceberg tables within Snowflake that directly reference the data in your S3 data lake. This makes the external data queryable without needing to move it, providing significant efficiency and cost benefits.

3. Refresh Iceberg Tables (Procedure: refresh_iceberg_tables)
As new data arrives in the S3 data lake, the metadata in Snowflake must be updated. This procedure synchronizes Snowflake with the latest state of your Iceberg tables, ensuring that any queries run against the tables are always up-to-date with the most recent data.

4. Load to Native Tables (Procedure: load_iceberg_to_snowflake_native)
For optimal performance and to leverage all of Snowflake's features, data is loaded from the external Iceberg tables into Snowflake's native storage format. This step prepares the data for more complex transformations and high-speed queries.

5. Transform Data (Procedure: TRANSFORM_RAW_TO_DIM_FACT)
This procedure transforms the raw, loaded data into a structured dimensional model (star schema). It efficiently handles updates and new records to create clean, analysis-ready Dimension and Fact tables that are optimized for reporting and analytics.

6. Analyze & Visualize
With the data cleaned and modeled, we can now derive powerful business insights. This stage presents a series of analytical queries and their visual representations, allowing users to explore key business metrics:

Sales Performance by Country: A bar chart that aggregates total sales amounts by the country of the product manufacturer, providing a high-level view of geographical performance.

Products with Low Inventory Levels: A table that identifies products where the quantity on hand has fallen below a pre-defined threshold, highlighting items that may need to be reordered.

Monthly Sales Trend: A line chart that displays total monthly sales and overlays a 3-month moving average to smooth out fluctuations and identify the underlying sales trend.

Top 5 Customers: A bar chart that ranks the top five customers based on their total lifetime spending, helping to identify and focus on high-value relationships.

Sales by Product Category: A bar chart that breaks down total sales revenue by product category, allowing for a clear comparison of performance across different segments of the business.

Features
Interactive Stepper: A clear, sequential navigation menu that guides users through each step of the pipeline.

Code Blocks: SQL code snippets are provided for each procedure, complete with a "Copy to Clipboard" button.

Dynamic Visualizations: The final "Analyze & Visualize" step features a tabbed interface with five different business insights, each represented by a chart or a table.

Responsive Design: The entire application is built with a mobile-first approach using Tailwind CSS to ensure it looks and works well on any device.
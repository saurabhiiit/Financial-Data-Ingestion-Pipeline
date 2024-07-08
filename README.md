# Financial-Data-Ingestion-Pipeline

## Introduction
The objective of this project to develop a comprehensive data ingestion pipeline that effectively ingest & manages financial price data of stocks. 

## Functionalities Implemented
**Data Ingestion**: 
    - Utilizes Apache Airflow to orchestrate the unzipping of daily datasets.
    - Processes each day's data concurrently.

**Data Cleaning, Transformation, and Optimization**: 
    - Removes ".NSE" from the Ticker column.
    - Combines Date and Time columns into a Timestamp column.
    - Converts columns to appropriate data types.

**Data Quality Checks and Observability**:
    - Identifies instruments with invalid values.
    - Compares unique Tickers with reference bhavcopy data.
    - Aggregates second-level data to day-level data and compares it with reference data.    

**Data Warehousing**: 
    - Inserts second-level data into PostgreSQL.
    - Optimizes the database schema for fast querying.

## Usage Instructions
- Provide with the location of the ticks data and bhavcopy data, rest code will take care. 

## Functionalities Not Implemented
    - Creates queries to fetch specific data points and generates aggregated ticks data based on user inputs.
    - Partitioning of the second_level table based on the timeframe. 

    

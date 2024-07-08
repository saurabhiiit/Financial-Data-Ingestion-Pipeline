from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import zipfile
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import time

def convert_folder_name(tick_folder_name):
    match = re.search(r'STOCK_TICK_(\d{2})(\d{2})(\d{4})', tick_folder_name)
    if match:
        day = match.group(1)
        month = match.group(2)
        year = match.group(3)
        
        month_map = {
            '01': 'JAN', '02': 'FEB', '03': 'MAR', '04': 'APR',
            '05': 'MAY', '06': 'JUN', '07': 'JUL', '08': 'AUG',
            '09': 'SEP', '10': 'OCT', '11': 'NOV', '12': 'DEC'
        }
        month_str = month_map[month]
        
        bhavcopy_folder_name = f"cm{day}{month_str}{year}bhav.csv"
        return bhavcopy_folder_name
    else:
        return None
default_args = {
    'owner': 'saurabh',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ticks_data = '/home/asus/data2/tb-data-engineer-assignment/ticks_data'  
bhavcopy_eodsnapshot_data = '/home/asus/data2/tb-data-engineer-assignment/bhavcopy_eodsnapshot_data'


# Define DAG
with DAG(
    dag_id='final_data_pipeline',
    default_args=default_args,
    description='A comprehensive data pipeline for financial price data',
    start_date=datetime(2024, 7, 2),
    schedule='@daily',
    catchup=False
) as dag:

    def extract_zip_files(zip_folder):
        extracted_folder = os.path.join(zip_folder, 'extracted_data')
        if not os.path.exists(extracted_folder):
            os.makedirs(extracted_folder)
        for zip_file in glob.glob(os.path.join(zip_folder, '*.zip')):
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(extracted_folder)
    def clean_transform_to_parquet(tick_folder):
        parquet_folder = os.path.join(tick_folder, 'parquet_data')
        extracted_folder = os.path.join(tick_folder, 'extracted_data')
       
        if not os.path.exists(parquet_folder):
            os.makedirs(parquet_folder)

        for date_folder in os.listdir(extracted_folder):
            # print(date_folder)
            parquet_fp = os.path.join(parquet_folder,date_folder)
            if not os.path.exists(parquet_fp): 
                os.makedirs(parquet_fp)
            csv_folder = os.path.join(extracted_folder, date_folder)
            for root, dirs, files in os.walk(csv_folder):
                for file in files:
                    if file.endswith('.csv'):
                        file_path = os.path.join(root, file)
                        df = pd.read_csv(file_path)
                        
                        df['Ticker'] = df['Ticker'].apply(lambda x: x.replace('.NSE', ''))
                        df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
                        
                        df['LTP'] = pd.to_numeric(df['LTP'], errors='coerce')
                        df['BuyPrice'] = pd.to_numeric(df['BuyPrice'], errors='coerce')
                        df['BuyQty'] = pd.to_numeric(df['BuyQty'], errors='coerce')
                        df['SellPrice'] = pd.to_numeric(df['SellPrice'], errors='coerce')
                        df['SellQty'] = pd.to_numeric(df['SellQty'], errors='coerce')
                        df['LTQ'] = pd.to_numeric(df['LTQ'], errors='coerce')
                        df['OpenInterest'] = pd.to_numeric(df['OpenInterest'], errors='coerce')

                        df = df[['Ticker', 'Timestamp', 'LTP', 'BuyPrice', 'BuyQty', 'SellPrice', 'SellQty', 'LTQ', 'OpenInterest']]
                        
                        table = pa.Table.from_pandas(df)
                        parquet_file_path = os.path.join(parquet_fp, f'{os.path.splitext(file)[0]}.parquet')
                        pq.write_table(table, parquet_file_path)
    
    def data_quality_checks(ticks_data,bhavcopy_eodsnapshot_data):
        # bhavcopy_eodsnapshot_data = '/home/asus/data2/tb-data-engineer-assignment/bhavcopy_eodsnapshot_data'

        parts = ticks_data.split(os.sep)
        parts[-1] = 'quality_test_data'
        quality_folder = os.sep.join(parts)
        if not os.path.exists(quality_folder):
                os.makedirs(quality_folder)
            
        extracted_folder = os.path.join(bhavcopy_eodsnapshot_data, 'extracted_data')
        parquet_folder = os.path.join(ticks_data, 'parquet_data')
        # print(parquet_folder)
        for date_folder in os.listdir(parquet_folder):
            print(date_folder)
            tick_csv_folder = os.path.join(parquet_folder, date_folder)
        
            bhavcopy_date_folder = convert_folder_name(date_folder)

            # bhavcopy_csv_folder = os.path.join(extracted_folder, bhavcopy_date_folder)
            # for file in os.listdir(bhavcopy_csv_folder):
            #     if file.endswith('.csv'):
            bhavcopy_file = os.path.join(extracted_folder,bhavcopy_date_folder)
                    # print(bhavcopy_file)

            bhavcopy_df = pd.read_csv(bhavcopy_file)
            bhavcopy_tickers = set(bhavcopy_df['SYMBOL'].unique())
            
            output_folder = os.path.join(quality_folder, date_folder)
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
            
            invalid_values_report = []
            missing_tickers = []
            summary_report = []

            print(tick_csv_folder)
            for root, dirs, files in os.walk(tick_csv_folder):
                for file in files:
                    if file.endswith('.parquet'):
                        file_path = os.path.join(root, file)
                        df = pd.read_parquet(file_path)
                        
                        invalid_values = df[(df['LTP'].isnull()) | (df['LTP'] < 0) |
                                            (df['BuyPrice'].isnull()) | (df['BuyPrice'] < 0) |
                                            (df['BuyQty'].isnull()) | (df['BuyQty'] < 0) |
                                            (df['SellPrice'].isnull()) | (df['SellPrice'] < 0) |
                                            (df['SellQty'].isnull()) | (df['SellQty'] < 0) |
                                            (df['LTQ'].isnull()) | (df['LTQ'] < 0) |
                                            (df['OpenInterest'].isnull()) | (df['OpenInterest'] < 0)]
                        if not invalid_values.empty:
                            invalid_values_report.append(file)


                        data_tickers = set(df['Ticker'].unique())
                        missing_tickers_in_data = bhavcopy_tickers - data_tickers
                        # print(missing_tickers_in_data)
                        if missing_tickers_in_data:
                            missing_tickers.append(missing_tickers_in_data)
                
                        df_aggregated = df.groupby('Ticker').agg({
                            'LTP': ['first', 'max', 'min', 'last'],
                            'BuyQty': 'sum',
                            'SellQty': 'sum'
                        })
                        df_aggregated.columns = ['Open', 'High', 'Low', 'Close', 'TotalBuyQty', 'TotalSellQty']
                        df_aggregated.reset_index(inplace=True)
                        
                        merged_df = pd.merge(df_aggregated, bhavcopy_df, left_on='Ticker', right_on='SYMBOL')
                        comparison = merged_df[['Ticker', 'Open', 'High', 'Low', 'Close', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]
                        # print(comparison)
                        comparison = comparison.copy()
                        comparison['Open_diff'] = comparison['Open'] - comparison['OPEN']
                        comparison['High_diff'] = comparison['High'] - comparison['HIGH']
                        comparison['Low_diff'] = comparison['Low'] - comparison['LOW']
                        comparison['Close_diff'] = comparison['Close'] - comparison['CLOSE']
                        print( comparison['Close_diff'])
                        summary_report.append(comparison)


                invalid_values_df = pd.DataFrame(invalid_values_report, columns=['File'])
                invalid_values_df.to_csv(os.path.join(output_folder, f'invalid_values_report_{date_folder}.csv'), index=False)
                
                flattened_missing_tickers = [ticker for sublist in missing_tickers for ticker in sublist]
                missing_tickers_df = pd.DataFrame(flattened_missing_tickers, columns=['MissingTickers'])
                missing_tickers_df.to_csv(os.path.join(output_folder, f'missing_tickers_report_{date_folder}.csv'), index=False)
            
                # missing_tickers_df = pd.DataFrame(missing_tickers, columns=['MissingTickers'])
                # missing_tickers_df.to_csv(os.path.join(output_folder, f'missing_tickers_report_{date_folder}.csv'), index=False)
            
                summary_report_df = pd.concat(summary_report)
                summary_report_df.to_csv(os.path.join(output_folder, f'summary_report_{date_folder}.csv'), index=False)
   
    def create_table():
        hook = PostgresHook(postgres_conn_id='my_conn')
        retries = 5
        for i in range(retries):
            try:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS second_level_data (
                    source_symbol VARCHAR,
                    timestamp TIMESTAMP,
                    ltp NUMERIC,
                    ltq NUMERIC,
                    oi NUMERIC,
                    bid NUMERIC,
                    bid_qty NUMERIC,
                    ask NUMERIC,
                    ask_qty NUMERIC
                );
                """
                hook.run(create_table_sql)
                break
            except psycopg2.OperationalError as e:
                if i < retries - 1:
                    time.sleep(5) 
                else:
                    raise e

    def transform_and_insert_data(base_folder):
        data_folder = os.path.join(base_folder,'extracted_data')
        hook = PostgresHook(postgres_conn_id='my_conn')
        engine = hook.get_sqlalchemy_engine()

        for root, dirs, files in os.walk(data_folder):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    df = pd.read_csv(file_path)
                    
                    df['Ticker'] = df['Ticker'].apply(lambda x: x.replace('.NSE', ''))
                    df['Timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
                    
                    df = df.rename(columns={
                        'Ticker': 'source_symbol',
                        'Timestamp': 'timestamp',
                        'LTP': 'ltp',
                        'LTQ': 'ltq',
                        'OpenInterest': 'oi',
                        'BuyPrice': 'bid',
                        'BuyQty': 'bid_qty',
                        'SellPrice': 'ask',
                        'SellQty': 'ask_qty'
                    })

                    df['ltp'] = pd.to_numeric(df['ltp'], errors='coerce')
                    df['ltq'] = pd.to_numeric(df['ltq'], errors='coerce')
                    df['oi'] = pd.to_numeric(df['oi'], errors='coerce')
                    df['bid'] = pd.to_numeric(df['bid'], errors='coerce')
                    df['bid_qty'] = pd.to_numeric(df['bid_qty'], errors='coerce')
                    df['ask'] = pd.to_numeric(df['ask'], errors='coerce')
                    df['ask_qty'] = pd.to_numeric(df['ask_qty'], errors='coerce')

                    df = df[['source_symbol', 'timestamp', 'ltp', 'ltq', 'oi', 'bid', 'bid_qty', 'ask', 'ask_qty']]
                    with engine.begin() as connection:
                        df.to_sql('second_level_data', con=connection, if_exists='append', index=False)
    def create_indexes():
        hook = PostgresHook(postgres_conn_id='my_conn')
        hook.run("""
        CREATE INDEX IF NOT EXISTS idx_source_symbol ON second_level_data(source_symbol);
        CREATE INDEX IF NOT EXISTS idx_timestamp ON second_level_data(timestamp);
        """)

    def identify_price_change():
        hook = PostgresHook(postgres_conn_id='my_conn')
        query = """
        WITH price_changes AS (
            SELECT
                source_symbol,
                MAX(CASE WHEN DATE(timestamp) = '2022-04-04' THEN ltp END) AS ltp_april_4,
                MAX(CASE WHEN DATE(timestamp) = '2022-04-05' THEN ltp END) AS ltp_april_5
            FROM
                second_level_data
            WHERE
                DATE(timestamp) IN ('2022-04-04', '2022-04-05')
            GROUP BY
                source_symbol
        )
        SELECT
            source_symbol,
            ltp_april_4,
            ltp_april_5,
            ((ltp_april_5 - ltp_april_4) / ltp_april_4) * 100 AS percentage_change
        FROM
            price_changes
        WHERE
            ((ltp_april_5 - ltp_april_4) / ltp_april_4) * 100 > 3;
        """
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
            print(f"Symbol: {row[0]}, Change: {row[3]:.2f}%")
    
    def create_materialized_view():
        hook = PostgresHook(postgres_conn_id='my_conn')
        hook.run("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_ohlc AS
        WITH hourly_data AS (
            SELECT
                source_symbol,
                date_trunc('hour', timestamp) AS hour,
                first_value(ltp) OVER (PARTITION BY source_symbol, date_trunc('hour', timestamp) ORDER BY timestamp) AS open,
                last_value(ltp) OVER (PARTITION BY source_symbol, date_trunc('hour', timestamp) ORDER BY timestamp) AS close,
                MAX(ltp) OVER (PARTITION BY source_symbol, date_trunc('hour', timestamp)) AS high,
                MIN(ltp) OVER (PARTITION BY source_symbol, date_trunc('hour', timestamp)) AS low
            FROM
                second_level_data
        )
        SELECT DISTINCT
            source_symbol,
            hour,
            open,
            high,
            low,
            close
        FROM
            hourly_data;
        """)
    
    def aggregate_ticks_data(agg_folder, frequency, from_date, to_date, symbols):
        hook = PostgresHook(postgres_conn_id='my_conn')
        conn = hook.get_conn()

        freq_map = {
            '5min': '5 minutes',
            '15min': '15 minutes',
            '1hour': '1 hour'
        }
        interval = freq_map.get(frequency, '1 hour')
        query = f"""
        SELECT 
            source_symbol,
            date_trunc('{interval}', timestamp) AS interval,
            first_value(ltp) OVER (PARTITION BY source_symbol, date_trunc('{interval}', timestamp) ORDER BY timestamp) AS open,
            MAX(ltp) OVER (PARTITION BY source_symbol, date_trunc('{interval}', timestamp)) AS high,
            MIN(ltp) OVER (PARTITION BY source_symbol, date_trunc('{interval}', timestamp)) AS low,
            last_value(ltp) OVER (PARTITION BY source_symbol, date_trunc('{interval}', timestamp) ORDER BY timestamp) AS close
        FROM 
            second_level_data
        WHERE
            timestamp BETWEEN '{from_date}' AND '{to_date}'
        GROUP BY 
            source_symbol
        """
            # source_symbol, interval;

        results = hook.get_records(query)

        aggregated_df = pd.DataFrame(results, columns=['source_symbol', 'interval', 'open', 'high', 'low', 'close'])
        aggregated_file = os.path.join(agg_folder,'extracted_data')
        aggregated_df.to_csv('/path/to/save/aggregated_data.csv', index=False)

     # Define tasks
    extract_ticks_data = PythonOperator(
        task_id='extract_ticks_data',
        python_callable=extract_zip_files,
        op_kwargs={
            'zip_folder': ticks_data
        }
    )

    transform_ticks_data = PythonOperator(
                task_id=f'clean_data',
                python_callable=clean_transform_to_parquet,
                 op_kwargs={
            'tick_folder': ticks_data
        }
            )
    
    extract_bhavcopy_data = PythonOperator(
        task_id='extract_bhavcopy_data',
        python_callable=extract_zip_files,
        op_kwargs={
            'zip_folder': bhavcopy_eodsnapshot_data
        }
    )

    data_quality_task = PythonOperator(
        task_id='data_quality_checks',
        python_callable=data_quality_checks,
        op_kwargs={
            'ticks_data': ticks_data,
            'bhavcopy_eodsnapshot_data': bhavcopy_eodsnapshot_data
        }
    )

    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    transform_and_insert_task = PythonOperator(
        task_id='transform_and_insert_data',
        python_callable=transform_and_insert_data,
        op_kwargs={
            'base_folder': ticks_data
        }
    )

    create_indexes_task = PythonOperator(
        task_id='create_indexes',
        python_callable=create_indexes
    )

    identify_price_change_task = PythonOperator(
        task_id='identify_price_change',
        python_callable=identify_price_change
    )

    create_materialized_view_task = PythonOperator(
        task_id='create_materialized_view',
        python_callable=create_materialized_view
    )

    aggregate_task = PythonOperator(
        task_id='aggregate_ticks_data',
        python_callable=aggregate_ticks_data,
        op_kwargs={
        'agg_folder': ticks_data,
        'frequency': '{{ dag_run.conf.get("frequency", "1hour") }}',
        'from_date': '{{ dag_run.conf.get("from_date", "2022-04-04") }}',
        'to_date': '{{ dag_run.conf.get("to_date", "2022-04-05") }}',
        'symbols': '{{ dag_run.conf.get("symbols", ["ALL"]) }}'
        
        }
    )
    # clean_task

    extract_ticks_data >> transform_ticks_data
    [transform_ticks_data, extract_bhavcopy_data] >> data_quality_task
    data_quality_task >> create_table_task >> transform_and_insert_task >> create_indexes_task >>  identify_price_change_task >>create_materialized_view_task

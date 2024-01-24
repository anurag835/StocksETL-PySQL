from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
# BSE(Bombay Stock Exchange) library is using here, where getQuote 
# function is request for script code to get the particular data of 
# security code that will return key value pairs.
from bsedata.bse import BSE
import time
import psycopg2 # for sql queries
from sqlalchemy import create_engine, inspect# pre-built toolkit to work with sql database.
import sqlalchemy as sa
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dotenv import load_dotenv
# Load environment variables from .env
load_dotenv()

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='stocks_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Indian Stocks Data',
    schedule_interval=timedelta(days=1),
    catchup=False
)

def extract_data(**kwargs):
    try:
        excel_url = "https://raw.githubusercontent.com/jangid6/Stock-ETL-Project/main/Equity.xlsx"  
        EquityDF = pd.read_excel(excel_url, engine='openpyxl')
        logging.info("Reading of the data from API is successful")
        logging.info(len(EquityDF))
    except Exception as e:
        logging.error(f"Error during data extraction: {e}")

# CALLING BSE API for fetching Stocks Data for eg: Price, Code, Updated Date, Open Price, Close Price, Mrkt Cap
    # Created a list of 50 stocks which is a part of NIFTY 50.
    listOf_Nifty50_StockIDs = [ 
    "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BPCL", "BHARTIARTL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB", "DRREDDY", "EICHERMOT",
    "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
    "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL",
    "KOTAKBANK", "LTIM", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND",
    "ONGC", "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA",
    "TCS", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TECHM", "TITAN",
    "UPL", "ULTRACEMCO", "WIPRO"
]

    # Filter EquityDF by looking up 'Security Id' col values exist in listOf_Nifty50_StockIDs 
    EquityDF['Security Code'] = EquityDF['Security Code'].astype(str) # Tranforming Security code i.e: Numeric col into 
                                                                      # str because of getQuote() accepts str as a input.

    # Here I'm filtering EquityDF because I don't need 4000. I just need 50, so i'm filtering by using isin().
    nifty50_OnlyDF= EquityDF[EquityDF['Security Id'].isin(listOf_Nifty50_StockIDs)].reset_index(drop=True)
    logging.info(len(nifty50_OnlyDF))
    nifty50_OnlyDF.rename(columns={'Group': 'CompanyGroup'}, inplace=True)
    nifty50_OnlyDF.columns = nifty50_OnlyDF.columns.str.replace(' ', '')
    nifty50_OnlyDF.head()

    # creating Bse Lib Object
    bseObject = BSE(update_codes=True)
    result_dfs = []
    sqcode_ListNifty50 = nifty50_OnlyDF['SecurityCode'].values
    
    for sqCode in sqcode_ListNifty50:
        try:
            # stock_Data is the item that is returned from the API. This is API return data.
            stock_Data = bseObject.getQuote(sqCode) # return key-value pair
            stock_df = pd.DataFrame([stock_Data])
            result_dfs.append(stock_df)
            # Introduce a small delay between API calls to their server. This precaution helps
            # prevent server issues and ensures a smoother interaction with their network.
            time.sleep(0.5) # Note:- time delay to avoid potential issues with the Bombay Stock Exchange (BSE) blocking my
                            # requests. BSE tends to block users who generate high network traffic, causing server overload.
        except IndexError:
            logging.error(f"IndexError for {sqCode}: Data not available")

    nifty50DailyTable = pd.concat(result_dfs, ignore_index=True).iloc[:,:-2]
    nifty50DailyTable.head()
    # Convert the DataFrame to a JSON-serializable format
    nifty50_data_dict = nifty50DailyTable.to_dict(orient='records')
    # Push the JSON-serializable data to XCom
    kwargs['ti'].xcom_push(key='nifty50_data', value=nifty50_data_dict)

# Data Pre-Processing (Cleaning and Transformation) for Microsoft SQL Server Database
def transform_data(**kwargs):
    task_instance = kwargs.get('ti')
    nifty50_data_dict = task_instance.xcom_pull(task_ids='extract_data', key='nifty50_data')
    if nifty50_data_dict is None:
        logging.warning("No data found in XCom. Check if the upstream task executed successfully.")
        return
    logging.info("XCom Value: %s", nifty50_data_dict)
    # Convert the JSON-serializable data back to a DataFrame
    nifty50DailyTable = pd.DataFrame.from_records(nifty50_data_dict)
    # Here, I'm renaming the column because 'group' is a reserved keyword in SQL queries,  
    # and I also prefer using descriptive alphabetic names instead of alphanumeric ones.

    # Data-Transformation Part
    nifty50DailyTable.rename(columns={'group': 'sharegroup'}, inplace=True)
    nifty50DailyTable.rename(columns={'52weekHigh': 'fiftytwoweekHigh'}, inplace=True)
    nifty50DailyTable.rename(columns={'52weekLow': 'fiftytwoweekLow'}, inplace=True)
    nifty50DailyTable.rename(columns={'2WeekAvgQuantity': 'twoWeekAvgQuantity'}, inplace=True)
    nifty50DailyTableTest_DF = nifty50DailyTable.copy()

    # In Existing dataframe column i.e: updateOn. It's not really a correct date format. 
    # Convert 'updatedOn' column to datetime and extract date
    nifty50DailyTableTest_DF['updatedOn'] = pd.to_datetime(nifty50DailyTableTest_DF['updatedOn'], format='%d %b %y | %I:%M %p', errors='coerce')
    
    # Check if there are any invalid or missing date values.
    if pd.isna(nifty50DailyTableTest_DF['updatedOn']).any():
        logging.warning("There are invalid or missing date values in the 'updatedOn' column.")
    else:
        # Extract data from 'updateOn' column & convert the column to datetime.
        nifty50DailyTableTest_DF['updatedOn'] = pd.to_datetime(nifty50DailyTableTest_DF['updatedOn'].dt.date)

    # Convert Timestamp objects to strings
    nifty50DailyTableTest_DF['updatedOn'] = nifty50DailyTableTest_DF['updatedOn'].astype(str)

    # Data-Cleaning part
    if 'totalTradedValueCr' not in nifty50DailyTableTest_DF.columns:
        nifty50DailyTableTest_DF['totalTradedValueCr'] = pd.to_numeric(nifty50DailyTableTest_DF['totalTradedValue'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')
        nifty50DailyTableTest_DF['totalTradedQuantityLakh'] = pd.to_numeric(nifty50DailyTableTest_DF['totalTradedQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')
        nifty50DailyTableTest_DF['twoWeekAvgQuantityLakh'] = pd.to_numeric(nifty50DailyTableTest_DF['twoWeekAvgQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')
        nifty50DailyTableTest_DF['marketCapFullCr'] = pd.to_numeric(nifty50DailyTableTest_DF['marketCapFull'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')
        nifty50DailyTableTest_DF['marketCapFreeFloatCr'] = pd.to_numeric(nifty50DailyTableTest_DF['marketCapFreeFloat'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')

        nifty50DailyTableTest_DF.drop(['totalTradedValue', 'totalTradedQuantity','twoWeekAvgQuantity', 'marketCapFull', 'marketCapFreeFloat'], axis=1, inplace=True)
    
    # Convert the DataFrame to a JSON-serializable format
    nifty50_data_dict_transformed = nifty50DailyTableTest_DF.to_dict(orient='records')
    
    # Push the JSON-serializable data to XCom
    kwargs['ti'].xcom_push(key='nifty50_data_transformed', value=nifty50_data_dict_transformed)

    # Time Series Plot
    # utilizes Seaborn and Matplotlib to generate a time series plot of stock prices and 
    # save it as a PNG file that includes the date on the x-axis and the stock prices on the y-axis.
    plt.figure(figsize=(12, 6))
    # Creates a line plot using Seaborn
    sns.lineplot(x='updatedOn', y='currentValue', data=nifty50DailyTableTest_DF, label='Stock Prices', marker='o')
    plt.title('Nifty 50 Stock Prices Over Time')
    plt.xlabel('Date')
    plt.ylabel('Stock Price (INR)')
    plt.legend()
    save_path = os.path.join(os.getcwd(), 'time_series_plot.png')
    plt.savefig(save_path)
    plt.close()
    
# Loading data to PostgreSQL Database Using psycopg2 connection and Sqlalchemy Engine
def load_data_to_postgresql(**kwargs):
    task_instance = kwargs.get('ti')
    nifty50DailyTableTest_data = task_instance.xcom_pull(task_ids='transform_data', key='nifty50_data_transformed')
    if nifty50DailyTableTest_data is None:
        logging.warning("No data found in XCom. Check if the upstream task executed successfully.")
        return

    nifty50DailyTableTest_DF=pd.DataFrame.from_records(nifty50DailyTableTest_data)
    nifty50DailyTableTest_DF['updatedOn'] = pd.to_datetime(nifty50DailyTableTest_DF['updatedOn'])
    logging.info("Original Column Names: %s", nifty50DailyTableTest_DF.columns)

    database = os.environ.get('DB_NAME')
    username = os.environ.get('DB_USER')
    password = os.environ.get('DB_PASSWORD')

    if database is None or username is None or password is None:
        raise ValueError("DB_NAME or DB_USER or DB_PASSWORD not found in environment variables.")
 
    # PostgreSQL database connection string
    conn_str = f'postgresql://{username}:{password}@localhost:5432/{database}'
    logging.info("Connection String: %s", conn_str)

    try:
        # Try to connect to the PostgreSQL using the engine
        conn=psycopg2.connect(conn_str)
        logging.info("Connection successful!")
        cursor = conn.cursor()
        engine = create_engine(conn_str, echo=True)

        # Create an inspector
        inspector = inspect(engine)
        nifty50_table_name = 'nifty50_dailydata'
        if not inspector.has_table(nifty50_table_name):
            nifty50_table_schema = f'''
            CREATE TABLE {nifty50_table_name} (
                "companyName" VARCHAR,
                "currentValue" NUMERIC,
                "change" NUMERIC,
                "pChange" NUMERIC,
                "updatedOn" TIMESTAMP,
                "securityID" VARCHAR,
                "scripCode" VARCHAR,
                "sharegroup" VARCHAR,
                "faceValue" NUMERIC,
                "industry" VARCHAR,
                "previousClose" NUMERIC,
                "previousOpen" NUMERIC,
                "dayHigh" NUMERIC,
                "dayLow" NUMERIC,
                "fiftytwoweekHigh" NUMERIC,
                "fiftytwoweekLow" NUMERIC,
                "weightedAvgPrice" NUMERIC,
                "totalTradedValueCr" NUMERIC,
                "totalTradedQuantityLakh" NUMERIC,
                "twoWeekAvgQuantityLakh" NUMERIC,
                "marketCapFullCr" NUMERIC,
                "marketCapFreeFloatCr" NUMERIC
            );
            '''
            # Execute the schema to create the table
            cursor.execute(nifty50_table_schema)
            conn.commit()
            cursor.close()
            conn.close()

        with engine.begin() as engineConn:
            # Check the maximum 'updatedOn' date in the existing SQL table
            sql_max_updatedOn = pd.read_sql_query(sa.text(f'SELECT MAX("updatedOn") FROM {nifty50_table_name}'), engineConn).iloc[0, 0]
            logging.info("Maximum updatedOn date:  %s",sql_max_updatedOn)
            
            # Check the maximum 'updatedOn' date in the DataFrame
            if nifty50DailyTableTest_DF is not None:
                df_max_updatedOn = nifty50DailyTableTest_DF['updatedOn'].max()
                logging.info(df_max_updatedOn)

            # Compare the dates and decide whether to append new data
            if (pd.isnull(sql_max_updatedOn)) and (not pd.isnull(df_max_updatedOn)):
                nifty50DailyTableTest_DF.to_sql(nifty50_table_name, engine, index=False, if_exists='append', method='multi')
                logging.info("Daily Data didn't exist, but now inserted successfully.")
            else:
                if (df_max_updatedOn > pd.Timestamp(sql_max_updatedOn)):
                    nifty50DailyTableTest_DF.to_sql(nifty50_table_name, engine, index=False, if_exists='append', method='multi')
                    logging.info("Data appended successfully.")
                else:
                    logging.info("No new data to append")
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL database: {e}")

# Define tasks in the DAG
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_postgresql',
    python_callable=load_data_to_postgresql,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task

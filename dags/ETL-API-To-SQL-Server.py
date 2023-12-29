from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
# BSE(Bombay Stock Exchange) library is using here, where getQuote 
# function is request for script code to get the particular data of 
# security code that will return key value pairs.
from bsedata.bse import BSE
import time
import pyodbc # for sql queries
from sqlalchemy import create_engine, inspect # pre-built toolkit to work with sql database.
from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy as sa
import logging

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2023, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
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
    excel_url = "https://raw.githubusercontent.com/jangid6/Stock-ETL-Project/main/Equity.xlsx"  
    EquityDF = pd.read_excel(excel_url, engine='openpyxl')
    print("Reading of the data from API is successful")
    logging.info("Reading of the data from API is successful")
    print(EquityDF.head(n=2))
    logging.info(len(EquityDF))

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
    print("EquityDF:>>>>",(len(nifty50_OnlyDF)))
    logging.info(len(nifty50_OnlyDF))
    nifty50_OnlyDF.columns = nifty50_OnlyDF.columns.str.replace(' ', '')
    nifty50_OnlyDF.head()
    nifty50_OnlyDF['SecurityCode'].values
    # creating Bse Lib Object
    bseObject = BSE(update_codes=True)
    listof_stockDicts = []
    sqcode_ListNifty50 = nifty50_OnlyDF['SecurityCode'].values
    
    for sqCode in sqcode_ListNifty50:
        try:
            # stockDict is the item that is returned from the API. This is API return data.
            stockDict = bseObject.getQuote(sqCode) # return key-value pair
            stockDict.pop("buy", None)
            stockDict.pop("sell", None)
            listof_stockDicts.append(stockDict)
            # Introduce a small delay between API calls to their server. This precaution helps
            # prevent server issues and ensures a smoother interaction with their network.
            time.sleep(0.5) # Note:- time delay to avoid potential issues with the Bombay Stock Exchange (BSE) blocking my
                            # requests. BSE tends to block users who generate high network traffic, causing server overload.
        except IndexError:
            logging.error(f"IndexError for {sqCode}: Data not available")

    nifty50DailyTable = pd.DataFrame(listof_stockDicts)
    nifty50DailyTable.head()
    # We can store nifty50DailyTable in XCom for use in the next task
    kwargs['ti'].xcom_push(key='nifty50_data', value=nifty50DailyTable)

# Data Pre-Processing (Cleaning and Transformation) for Microsoft SQL Server Database
def transform_data(**kwargs):
    task_instance = kwargs['ti']
    nifty50DailyTable = task_instance.xcom_pull(task_ids='extract_data', key='nifty50_data')
    # Here, I'm renaming the column because 'group' is a reserved keyword in SQL queries,  
    # and I also prefer using descriptive alphabetic names instead of alphanumeric ones.

    # Data-Transformation Part
    nifty50DailyTable.rename(columns={'group': 'sharegroup'}, inplace=True)
    nifty50DailyTable.rename(columns={'52weekHigh': 'fiftytwoweekHigh'}, inplace=True)
    nifty50DailyTable.rename(columns={'52weekLow': 'fiftytwoweekLow'}, inplace=True)
    nifty50DailyTable.rename(columns={'2WeekAvgQuantity': 'twoWeekAvgQuantity'}, inplace=True)
    # In Existing dataframe column i.e: updateOn. It's not really a correct date format
    nifty50DailyTable['updatedOn'] = pd.to_datetime(nifty50DailyTable['updatedOn'], format='%d %b %y | %I:%M %p', errors='coerce')

    # Check if there are any invalid or missing date values.
    if pd.isna(nifty50DailyTable['updatedOn']).any():
        logging.warning("There are invalid or missing date values in the 'updatedOn' column.")
    else:
        # Extract data from 'updateOn' column & convert the column to datetime.
        nifty50DailyTable['updatedOn'] = pd.to_datetime(nifty50DailyTable['updatedOn'].dt.date)

    # Data-Cleaning part
    if 'totalTradedValueCr' not in nifty50DailyTable.columns:
        nifty50DailyTable['totalTradedValueCr'] = pd.to_numeric(nifty50DailyTable['totalTradedValue'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')
        nifty50DailyTable['totalTradedQuantityLakh'] = pd.to_numeric(nifty50DailyTable['totalTradedQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')
        nifty50DailyTable['twoWeekAvgQuantityLakh'] = pd.to_numeric(nifty50DailyTable['twoWeekAvgQuantity'].str.replace(',', '').str.replace(' Lakh', '', regex=True), errors='coerce')
        nifty50DailyTable['marketCapFullCr'] = pd.to_numeric(nifty50DailyTable['marketCapFull'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')
        nifty50DailyTable['marketCapFreeFloatCr'] = pd.to_numeric(nifty50DailyTable['marketCapFreeFloat'].str.replace(',', '').str.replace(' Cr.', '', regex=True), errors='coerce')

        nifty50DailyTable.drop(['totalTradedValue', 'totalTradedQuantity','twoWeekAvgQuantity', 'marketCapFull', 'marketCapFreeFloat'], axis=1, inplace=True)

    nifty50DailyTable.head()

# Loading data to Microsoft SQL Server Database Using pyodbc connection and Sqlalchemy Engine
def load_data_to_sql_server(**kwargs):
    task_instance = kwargs['ti']
    nifty50DailyTable = task_instance.xcom_pull(task_ids='transform_data', key='nifty50_data')

    server = 'DESKTOP-A066HIG\SQLEXPRESS'
    database = 'nifty50'
    username = 'sa'
    password = 'sqlserver'
    driver = 'ODBC Driver 17 for SQL Server'
    
    # SQL database connection string
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    
    # Create an SQLAlchemy Engine
    engine = create_engine(f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}")

    def create_connection(conn_str):
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        return conn, cursor

    try:
        # Try to connect to the SQL Server using the engine
        connection = engine.connect()
        print("Connection successful!")
        logging.info("Connection successful!")
        connection.close()
        conn,  cursor = create_connection(conn_str)
        inspector = inspect(engine)
        nifty50_table_name = 'nifty50_dailydata'
        if not inspector.has_table(nifty50_table_name):
            nifty50_table_schema = f'''
            CREATE TABLE {nifty50_table_name} (
                companyName NVARCHAR(MAX),
                currentValue FLOAT,
                change FLOAT,
                pChange FLOAT,
                updatedOn DATE,
                securityID NVARCHAR(MAX),
                scripCode NVARCHAR(MAX),
                sharegroup NVARCHAR(MAX),
                faceValue FLOAT,
                industry NVARCHAR(MAX),
                previousClose FLOAT,
                previousOpen FLOAT,
                dayHigh FLOAT,
                dayLow FLOAT,
                fiftytwoweekHigh FLOAT,
                fiftytwoweekLow FLOAT,
                weightedAvgPrice FLOAT,
                totalTradedQuantityLakh FLOAT,
                totalTradedValueCr FLOAT,
                twoWeekAvgQuantityLakh FLOAT,
                marketCapFullCr FLOAT,
                marketCapFreeFloatCr FLOAT
            );
            '''
            # Execute the schema to create the table
            cursor.execute(nifty50_table_schema)
            conn.commit()
            conn.close()

        with engine.begin() as engineConn:
            # Check the maximum 'updatedOn' date in the existing SQL table
            sql_max_updatedOn = pd.read_sql_query(sa.text(f'SELECT MAX(updatedOn) FROM {nifty50_table_name}'), engineConn).iloc[0, 0]
            logging.info(sql_max_updatedOn)
            print(sql_max_updatedOn)
            # Check the maximum 'updatedOn' date in the DataFrame
            df_max_updatedOn = nifty50DailyTable['updatedOn'].max()
            logging.info(df_max_updatedOn)
            print(df_max_updatedOn)

            # Compare the dates and decide whether to append new data
            if (pd.isnull(sql_max_updatedOn)) and (not pd.isnull(df_max_updatedOn)):
                nifty50DailyTable.to_sql(nifty50_table_name, engine, index=False, if_exists='append', method='multi')
                logging.info("Daily Data didn't exist, but now inserted successfully.")
                print("Daily Data didn't exist, but now inserted successfully.")
            else:
                if (df_max_updatedOn > pd.Timestamp(sql_max_updatedOn)):
                    nifty50DailyTable.to_sql(nifty50_table_name, engine, index=False, if_exists='append', method='multi')
                    logging.info("Data appended successfully.")
                    print("Data appended successfully.")
                else:
                    logging.info("No new data to append")
                    print("No new data to append.")
    except SQLAlchemyError as e:
        logging.error(f"Error connecting to SQL Server: {e}")

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
    task_id='load_data_to_sql_server',
    python_callable=load_data_to_sql_server,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task

import httpx
from prefect import flow , get_run_logger, task
import psycopg2
import pandas as pd
from Scripts.country_mappings import currency_mapping
from prefect.deployments import run_deployment
from prefect.infrastructure import Process
from prefect.deployments import Deployment

#retrieve data from api
@task
def retrieve_from_api(
    base_url: str,
    endpoint: str,
    access_key: str ,
    secure: bool
):

    logger = get_run_logger()
    if secure:
        url = f"https://{base_url}/{endpoint}?access_key={access_key}"
    else:
        url= f"http://{base_url}/{endpoint}?access_key={access_key}"

   
    response = httpx.get(url)
    response.raise_for_status()
    data = response.json()
    logger.info(data)
    return data
 

# convert json_data to dataframe and add countries
@task
def transform_json_to_dataframe(data: dict, currency_mapping: dict):
    # get the rates dictionary from the response.json
    rates = data.get('rates', {})
     # get the country code and rate from the rates dictionary
    data1 = [(country, rate) for country, rate in rates.items()]
    # Create a DataFrame
    df = pd.DataFrame(data1, columns=['Currency_Code', 'Rate'])
    # Add the Country_Currency column
    df['Country_Currency'] = df['Currency_Code'].map(currency_mapping)
    df= df[['Country_Currency','Currency_Code', 'Rate']]
    return df
    
       
# load data into postgresql
# create database
@task
def create_database(
    db_name: str,
    db_user: str,
    db_pass: str,
    db_host:str,
    db_port: int    
):
    conn = psycopg2.connect(
        dbname= db_name,
        user= db_user,
        password= db_pass,
        host= db_host,
        port = db_port
    )

    conn.autocommit = True
    cursor = conn.cursor()

    print("Connected to 'postgres' database successfully!")
        
    try:
        cursor.execute("DROP DATABASE IF EXISTS db_prefect")
        cursor.execute("CREATE DATABASE db_prefect")
        print("Database 'prefect_db' created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print("Database 'db_prefect' already exists.")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating database:", error)
    

    cursor.close()
    conn.close()
        
@task        
def load_to_postgres(
    df,
    database_name: str,
    db_user: str,
    db_pass: str,
    db_host:str,
    db_port: int
):
    

    conn = psycopg2.connect(
        dbname= database_name,
        user= db_user,
        password= db_pass,
        host= db_host,
        port = db_port
    )
    
    cursor = conn.cursor()
    print("Connected to 'db_prefect' database successfully!")

    try:
        cursor.execute("DROP TABLE IF EXISTS prefect_table")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prefect_table (
                Country_Currency varchar(250) NOT NULL,
                Currency_Code varchar(10) NOT NULL,
                Rate numeric NOT NULL
            )
        """)
        print("Table 'prefect_table' created successfully.")
    except (Exception, psycopg2.Error) as error:
        print("Error while creating table:", error)

    Country_Currency = df['Country_Currency'].to_list()
    Currency_Code = df['Currency_Code'].to_list()
    Rate = df['Rate'].to_list()

    insert_query = """INSERT INTO prefect_table (Country_Currency, Currency_Code, Rate) VALUES (%s, %s, %s)"""
    for i in range(len(Country_Currency)):
        records = (Country_Currency[i], Currency_Code[i], Rate[i])
        cursor.execute(insert_query, records)
    
    conn.commit()
    print("Data inserted successfully!")

    cursor.close()
    conn.close()
    print("Connection Closed!")

@flow
def etl_flow(
    base_url: str = 'api.exchangeratesapi.io/v1',
    endpoint: str = 'latest',
    access_key: str = 'write your access key here',
    secure: bool = True,
    currency_mapping: dict = currency_mapping,
    db_name: str = 'postgres',
    db_user: str ='postgres',
    db_pass: str ='@write your password here',
    db_host:str = 'localhost',
    db_port: int = 5432,
    database_name: str = 'db_prefect'
):
    data = retrieve_from_api(base_url,endpoint,access_key,secure)
    df = transform_json_to_dataframe(data, currency_mapping)
    create_database(db_name, db_user, db_pass, db_host, db_port)
    load_to_postgres(df, database_name, db_user, db_pass, db_host, db_port)


if __name__ == "__main__":
    etl_flow()


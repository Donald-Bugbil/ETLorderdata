import boto3
import csv
import os
import io
import pandas as pd
import pendulum
import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base,Session
from sqlalchemy import Column, Integer,Float,Boolean, DateTime, String
from dotenv import load_dotenv
from airflow.decorators import dag, task


Base=declarative_base()
load_dotenv()
task_logger=logging.getLogger('workflow.task')

#s3 Bucket credentials
SECRET_KEY=os.environ['SECRET_KEY']
ACCESS_KEY=os.environ['ACCESS_KEY']
BUCKET_NAME=os.environ['BUCKET_NAME']

# #DB credentials
DRIVERNAME=os.environ['DRIVERNAME']
POSTGRES_PASSWORD=os.environ['POSTGRES_PASSWORD']
POSTGRES_USER=os.environ['POSTGRES_USER']
POSTGRES_DB=os.environ['POSTGRES_DB']
HOST=os.environ['HOST']

#create the database configuration
database_configuration=URL.create(drivername=DRIVERNAME,
                                  username=POSTGRES_USER, 
                                  password=POSTGRES_PASSWORD,
                                  host=HOST, 
                                  database=POSTGRES_DB
                                  )
#start the engine
engine=create_engine(database_configuration)

#The order table created to insert into the database
class Order(Base):
    __tablename__="order"
    id=Column(Integer, primary_key=True, autoincrement=True)
    order_id=Column(Integer)
    order_date=Column(DateTime)
    customer_id=Column(String)
    customer_name=Column(String)
    email=Column(String)
    product=Column(String)
    product_category=Column(String)
    quantity=Column(Float)
    price_usd=Column(Float)
    country=Column(String)
    state=Column(String)
    invalid_email=Column(Boolean)
    clv=Column(Float)
    new_or_returning_customer=Column(String)

    def __repr__(Self):
        return {Self.order_id}
#initializing the database   
def database_initiliaze():
    return Base.metadata.create_all(engine)


#starting dag
@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2025, 6, 7, tz="UTC"),
    catchup=True,
    tags=['aws_workflow']
)

#workflow
def workflow():
    """
    This workflow extract raw data from s3 bucket,transform the data and load to postgres  database
    """

    @task()
    #This is task test the connection of the database
    def database_initialization():
        try:
            database_initiliaze()
            task_logger.info(f"database initialization conneceted successfully:{True}")
            return True
        except Exception as e:
            task_logger.error(f"database unable to connect:{e}")

    @task()
    #This task extract data from S3
    def extract():
        session=boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        Client=session.client('s3')
        response=Client.get_object(Bucket=BUCKET_NAME, Key='orders_dataset.csv')['Body'].read( )
        
        #convert response into Bytes format
        Bytes_format=io.BytesIO(response)

        #create the df
        data_frame=pd.read_csv(Bytes_format)
        try:
            task_logger.info(f"DataFrame is generated successfully:{True}")
            return True
        except Exception as e:
            task_logger.error("error encountered generating DataFrame:{e}")
        return data_frame
    



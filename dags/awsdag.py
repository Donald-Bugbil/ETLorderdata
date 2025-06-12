import boto3
import csv
import os
import io
import re
import pandas as pd
import pendulum
import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import declarative_base,Session
from sqlalchemy import Column, Integer,Float,Boolean, DateTime, String
from dotenv import load_dotenv
from airflow.decorators import dag, task
from utilities.functions import name_converter, country_name, state_name,invalid_email, product_category


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
    new_or_returning=Column(String)

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
            return None

    @task()
    #This is the extract function to pull raw data in batches from aws s3 bucket
    def extract():
        
        try:
            session=boto3.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
            Client=session.client('s3')
            response=Client.get_object(Bucket=BUCKET_NAME, Key='orders_dataset.csv')['Body'].read( )
        
            #convert response into Bytes format
            Bytes_format=io.BytesIO(response)

            #create the df
            data_frame=pd.read_csv(Bytes_format)
            task_logger.info(data_frame)
            task_logger.info(f"DataFrame is generated successfully:{True}")
            return data_frame

        except Exception as e:
            task_logger.error("error encountered generating DataFrame:{e}")
       
    
    
    @task()
    #This is the tramsform function to clean and transformed the data to be data
    def transform(data_frame):
        new_data_frame=data_frame.copy()

        #Convert date format to YYYY-MM-DD
        new_data_frame['order_date']=pd.to_datetime(new_data_frame['order_date'], format='mixed', dayfirst=True)
        new_data_frame['order_date']=new_data_frame['order_date'].dt.strftime('%Y-%m-%d')

        #cleaning custoer_name column for format consistency
        new_data_frame['customer_name']=new_data_frame['customer_name'].apply(lambda x:name_converter(x))

        #formatting na in customer_name
        new_data_frame['customer_name']=new_data_frame['customer_name'].fillna(new_data_frame['email'])

        #cleaning country name format
        new_data_frame['country']=new_data_frame['country'].apply(lambda x:country_name(x))

        #cleaning state column
        new_data_frame['state']=new_data_frame['state'].apply(lambda x: state_name(x))

        #validating email
        new_data_frame['invalid_email']=new_data_frame['email'].apply(invalid_email)

        #cleaning price_usd(drop na)
        new_data_frame.dropna(subset=['price_usd'], inplace=True)

        #cleaning product_category 
        new_data_frame['product_category']=new_data_frame['product_category'].apply(lambda x:product_category(x))

        #formatting NaN from product category
        new_data_frame['product_category']=new_data_frame.groupby('country')['product_category'].transform(lambda x:x.fillna(x.mode()[0]))

  
        #formatting NaN in quantity
        new_data_frame['quantity']=new_data_frame.groupby('product_category')['quantity'].transform(lambda x:x.fillna(x.mode()[0]))
        #droped duplicated row based on order_id, row 2 has missing email hence dropped
        new_data_frame.loc[2] = new_data_frame.loc[24]
        new_data_frame.drop(index=24, inplace=True)

        #Adding CLV
        new_data_frame['clv']=new_data_frame.groupby('customer_id')['price_usd'].transform('sum')

        # new/returning flag

        # Convert order_date to datetime 
        new_data_frame['order_date'] = pd.to_datetime(new_data_frame['order_date'])

        # Find the first purchase date per customer
        first_order = new_data_frame.groupby('customer_id')['order_date'].transform('min')

        # Compare to the current order date
        new_data_frame['new_or_returning'] = new_data_frame['order_date'] == first_order
        new_data_frame['new_or_returning'] = new_data_frame['new_or_returning'].map({True: 'New', False: 'Returning'})

        task_logger.info(new_data_frame)
        
        return new_data_frame
    

    @task()
    #The loaded function to push the transformed data to posgres DB 
    def load(transformed_data, database_state):
        #An empty list to hold the populated data from each row
        data_to_insert=[]

        # Create and return an Order object from the DataFrame row with customer, product, and transaction details

        def create_object(row):
            order=Order(
                order_id=row['order_id'],
                order_date=row['order_date'],
                customer_id=row['customer_id'],
                customer_name=row['customer_name'],
                email=row['email'],
                product=row['product'],
                product_category=row['product_category'],
                quantity=row['quantity'],
                price_usd=row['price_usd'],
                country=row['country'],
                state=row['state'],
                invalid_email=row['invalid_email'],
                clv=row['clv'],
                new_or_returning=row['new_or_returning']


            )

            data_to_insert.append(order)

        task_logger.info(f"logging: {database_state}")
        if database_state is True:
            data_to_load=transformed_data
            task_logger.info(data_to_load)
            task_logger.info(f"Database is ready and starting to load data....")
            data_to_load.apply(lambda row:create_object(row), axis=1)

            with Session(engine) as session:
                session.add_all(data_to_insert)
                session.commit()
                task_logger.info(f"data loaded successfully")
                return "load complete"
        else:
            task_logger.warning(f"Database not initiliazed.skipping load")
            return "Skipped load due to Database error"

            

                
                        
    #calling the tasks
    Initiaze_DB=database_initialization()
    extraction=extract()
    transformation=transform(extraction)
    load(transformation, Initiaze_DB)
#calling the workflow of the dag
workflow()
                    
    




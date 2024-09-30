# importing the required modules
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Creating a spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# properties of the RDS table to connect
jdbc_url = "jdbc:mysql://<RDS_database_ARN>:3306/<database>"

db_properties = {
    "user": "username",
    "password": "password",
    "driver": "com.mysql.jdbc.Driver"
}

# Transforming the data to maintain standard and uniformity
def headers(df) : 
    headers = list(zip(df.columns, df.first()))
    for i in headers:
        df = df.withColumnRenamed(i[0], i[1])
    
    df = df.filter(df[0] != df.columns[0])

    for i in df.columns:
        df = df.withColumnRenamed(i, i.replace(" ", "_"))

    return df

# retrieving the data from Glue Catalog
orders_df = glueContext.create_dynamic_frame.from_catalog(
    database = "<Glue_catalog_database_name>", 
    table_name = "<table_name>"
)

returns_df = glueContext.create_dynamic_frame.from_catalog(
    database = "<Glue_catalog_database_name>", 
    table_name = "<table_name>"
)

# Converting the data into pyspark dataframe for further manipulations
orders_df = orders_df.toDF()
returns_df = returns_df.toDF()

orders_df = headers(orders_df)
returns_df = headers(returns_df)

# Joining the data based on the Order_ID column
joined_df = orders_df.join(
    returns_df,
    orders_df.Order_ID == returns_df.Order_ID,
    'left'
    ).drop(returns_df.Order_ID)

# Inserting the joined dataframe into the RDS table
joined_df.write.jdbc(
    url=jdbc_url,
    table="<RDS_table_name>",
    mode="overwrite",
    properties=db_properties
)
#!/usr/bin/env python
# coding: utf-8

# In[41]:


import requests
import os

try:
    bearer = os.environ['RIDEMOVI_BEARER']
except:
    raise Exception('could not find RIDEMOVI_BEARER environment variable')

url = 'https://api.ridemoviapp.com/mds-provider/gbfs/florence/free_bike_status.json'

headers = {
    'Authorization': f'Bearer {bearer}',
}

response = requests.get(url, headers=headers).json()
print('Data fetched. Example:')
print(response['data']['bikes'][0])


# In[35]:


from time import localtime
import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    FloatType,
    StringType,
    TimestampType,
    StructType,
    StructField,
    BooleanType,
    IntegerType,
)

# do some data cleaning
last_updated_time = datetime.datetime.fromtimestamp(response['last_updated'])
data = []
for record in response['data']['bikes']:
    record['last_update'] = last_updated_time
    record.pop('rental_uris')
    record['city'] = record['city']['name']
    record['lon'] = float(record['lon'])
    record['lat'] = float(record['lat'])
    data.append(record)

# create a spark dataframe
spark = SparkSession.builder.getOrCreate()
schema = StructType(
    [
        StructField("bike_id", StringType()),
        StructField("lon", FloatType()),
        StructField("lat", FloatType()),
        StructField("is_reserved", BooleanType()),
        StructField("is_disabled", BooleanType()),
        StructField("vehicle_status", StringType()),
        StructField("city", StringType()),
        StructField("soc", IntegerType()),
        StructField("pricing_plan_id", StringType()),
        StructField("vehicle_type_id", StringType()),
        StructField("current_range_meters", IntegerType()),
        StructField("model", StringType()),
        StructField("last_update", TimestampType()),
    ]
)
df = spark.createDataFrame(data, schema)
del data, response


# In[36]:


table_name = "ridemovi_bikes"

# you need to create a database before
db_name = 'mobility'
spark.sql("CREATE DATABASE IF NOT EXISTS " + db_name)

# create table if not exists and insert data
db_table = f'{db_name}.{table_name}'
if not spark.catalog.tableExists(db_table):
    df.writeTo(db_table).create()
    print("Table was not existing. Created from scratch")
else:
    df.writeTo(db_table).append()
    print("Table already exists")
print("Data added successfully")


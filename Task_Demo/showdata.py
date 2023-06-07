import requests
import json
from getdata import get_api_data, urls
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read JSON File") \
    .getOrCreate()

Output = {}

for key,value in urls.items():
    Output[key] = get_api_data(value)


# Read JSON file into a DataFrame
def spark_data():
    rating = spark.read.json(spark.sparkContext.parallelize([json.dumps(Output["rating"])]))
    appointment = spark.read.json(spark.sparkContext.parallelize([json.dumps(Output["appointment"])]))
    councillor = spark.read.json(spark.sparkContext.parallelize([json.dumps(Output["councillor"])]))
    patient_councillor = spark.read.json(spark.sparkContext.parallelize([json.dumps(Output["patient_councillor"])]))
    
    return rating,appointment,councillor,patient_councillor

rating, appointment, councillor,patient_councillor = spark_data()


# Show the DataFrame
rating.show(2)
appointment.show(2)
councillor.show(2)
patient_councillor.show(2)



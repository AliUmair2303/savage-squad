import requests
import json
from getdata import get_api_data, urls
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, collect_list,struct


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



def joined_data():
    
    rating, appointment, councillor,patient_councillor = spark_data()
    
    appointment_rating_df = rating.join(appointment, rating["appointment_id"] == appointment["id"], "inner").select(appointment['patient_id'],rating['value'])

    specialization_df = appointment_rating_df.join(patient_councillor, appointment_rating_df["patient_id"] == patient_councillor["patient_id"], "inner").select(appointment_rating_df['patient_id'],patient_councillor['councillor_id'], appointment_rating_df['value'])

    final_df = specialization_df.join(councillor, specialization_df["councillor_id"] == councillor["id"], "inner").select(councillor['id'],councillor['specialization'],specialization_df['value'])

    grouped_df = final_df.groupBy(final_df["id"],final_df["specialization"]).agg(avg(rating["value"]).alias("Avg-Rating-Value")).orderBy("Avg-Rating-Value", ascending = False)
    
    # final_group = grouped_df.groupBy(grouped_df["specialization"]).agg(collect_list(struct("id","Avg-Rating-value")).alias("Councillor_id_And_Rating"))

    # Create tables based on specialization
    specializations = [row.specialization for row in grouped_df.select("specialization").distinct().collect()]
    specializations_dict = {}
    for specialization in specializations:
        specializations_dict[specialization] = grouped_df.filter(grouped_df["specialization"] == specialization).drop("specialization").toJSON().collect()
    return specializations_dict
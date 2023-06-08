import requests
import json
from getdata import get_api_data, urls
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
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
# rating.show(2)
# appointment.show(2)
# councillor.show(2)
# patient_councillor.show(2)
appointment_rating_df = rating.join(appointment, rating["appointment_id"] == appointment["id"], "inner").select(appointment['patient_id'],rating['value'],rating['note'])


specialization_df = appointment_rating_df.join(patient_councillor, appointment_rating_df["patient_id"] == patient_councillor["patient_id"], "inner")

final_df = specialization_df.join(councillor, specialization_df["councillor_id"] == specialization_df["id"], "inner")

grouped_df = final_df.groupBy(councillor["id"],councillor["specialization"]).agg(avg(rating["value"]).alias("Avg Rating Value"))

# Avg_rating_list = grouped_df.collect()

# output_df = grouped_df.groupBy(grouped_df["specialization"])

# specialization = grouped_df.select('specialization').distinct().rdd.flatMap(lambda x: x).collect()


# print(specialization)
#Avg rating pe group by lagega specialization ki basis pe


# Show the result
#grouped_df.show(15)
# print(output_df)

sorted_df = grouped_df.orderBy("Avg_Rating_Value", ascending=False)

# Collect the specializations
specializations = sorted_df.select("specialization").rdd.flatMap(lambda x: x).collect()

# Print the specializations
for specialization in specializations:
    print(specialization)
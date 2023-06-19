import redis
import json
from showdata import joined_data

# Create a Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

redis_dict = joined_data()

def store_tables_in_redis(redis_dict):
    for specialization, df in redis_dict.items():
        # Convert list of JSON strings to JSON data
        json_data = [json.loads(row) for row in df]

        # Store JSON data in Redis
        redis_client.set(specialization, json.dumps(json_data))
        print(f"Table '{specialization}' stored in Redis.")


# def retrieve_tables_from_redis():
#     specializations = redis_client.keys("*")
#     for specialization in specializations:
#         specialization = specialization.decode()
#         stored_data = redis_client.get(specialization)
#         if stored_data:
#             print(f"Retrieved data from Redis for table '{specialization}':")
#             rows = json.loads(stored_data)
#             for row in rows:
#                 for column, value in row.items():
#                     print(f"{column}: {value}")
#                 print("---")
#         else:
#             print(f"No data found in Redis for table '{specialization}'.")

#Store tables in Redis
store_tables_in_redis(redis_dict)

# # Retrieve and print tables from Redis
# retrieve_tables_from_redis()

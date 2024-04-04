from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,TimestampType
from pyspark.sql.functions import col, datediff, expr, to_date, date_sub

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

data =  [
 (1, 101, 'login', '2023-09-05 08:30:00'),
 (2, 102, 'click', '2023-09-06 12:45:00'),
 (3, 101, 'click', '2023-09-07 14:15:00'),
 (4, 103, 'login', '2023-09-08 09:00:00'),
 (5, 102, 'logout', '2023-09-09 17:30:00'),
 (6, 101, 'click', '2023-09-10 11:20:00'),
 (7, 103, 'click', '2023-09-11 10:15:00'),
 (8, 102, 'click', '2023-09-12 13:10:00')
]

schema = StructType([
    StructField("log_id", IntegerType()),
    StructField("user$id",IntegerType()),
    StructField("action",StringType()),
    StructField("timestamp",StringType())
])

print("1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field")
my_dataframe=spark.createDataFrame(data=data,schema=schema)
my_dataframe.show()

print("2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function")
def renaming_columns(dataframe, new_names):
    for old_column, new_column in zip(dataframe.columns, new_names):
        dataframe = dataframe.withColumnRenamed(old_column, new_column)
    return dataframe

new_names = ["log_id", "user_id", "user_activity", "time_stamp"]
#renamed columns in the dataframe
new_df = renaming_columns(my_dataframe, new_names)
new_df.show()

# 3. Write a query to calculate the number of actions performed by each user in the last 7 days
print("3. Write a query to calculate the number of actions performed by each user in the last 7 days")
df_filtered = new_df.filter(datediff(expr("date('2023-09-12')"), expr("date(timestamp)")) <= 7)
actions_performed = df_filtered.groupby("user_id").count()
actions_performed.show()


#Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
print("4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type")
result_df = new_df.select("log_id", "user_id", "user_activity", to_date("time_stamp").alias("login_date"))
result_df.printSchema()
result_df.show()


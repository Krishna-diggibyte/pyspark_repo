from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import col, datediff, expr, to_date

def create_session():
    spark = SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()
    return spark

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

def create_df (spark, data,schema):
    df=spark.createDataFrame(data=data,schema=schema)
    return df

new_names = ["log_id", "user_id", "user_activity", "time_stamp"]
def renaming_columns(dataframe, new_names):
    for old_column, new_column in zip(dataframe.columns, new_names):
        dataframe = dataframe.withColumnRenamed(old_column, new_column)
    return dataframe

def action_count(df):
    df_filtered = df.filter(datediff(expr("date('2023-09-12')"), expr("date(timestamp)")) <= 7)
    actions_performed = df_filtered.groupby("user_id").count()
    return actions_performed


def update_column_login(df):
    result=df.select("log_id", "user_id", "user_activity",to_date("time_stamp").alias("login_date"))
    return result

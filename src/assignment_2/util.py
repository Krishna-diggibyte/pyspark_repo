from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,StructType,StructField

data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]

schema = StructType([
    StructField("card_number",StringType(),True)
])

csv_path="../../resource/card_number.csv"
def create_session():
    spark = SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()
    return spark


def create_df_direct(spark, data, schema):
    df=spark.createDataFrame(data,schema)
    return df

def create_df_infer(spark,path):
    df=spark.read.csv(path,inferSchema=True,header=True)
    return df

def create_df_custom(spark,path,schema):
    df=spark.read.csv(path,schema=schema,header=True)
    return df

def total_partition(df):
    total=df.rdd.getNumPartitions()
    return total

def inc_partition(df,no_of_partition):
    new_parti=df.rdd.repartition(no_of_partition+5)
    return new_parti

def dec_partition(df):
    old_parti=df.repartition(df.getNumPartitions()-5)
    return old_parti

def masked_card(card_number):
    masked_number = ('*'*(len(card_number)-4))+card_number[-4:]
    return masked_number

credit_mask_udf = udf(masked_card , StringType())


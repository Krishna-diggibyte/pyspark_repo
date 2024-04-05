from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col
from pyspark.sql.types import StringType,StructType,IntegerType,StructField

data_1=[(1, "iphone13"),
    (1, "dell i5 core"),
    (2, "iphone13"),
    (2, "dell i5 core"),
    (3, "iphone13"),
    (3, "dell i5 core"),
    (1, "dell i3 core"),
    (1, "hp i5 core"),
    (1, "iphone14"),
    (3, "iphone14"),
    (4, "iphone13")]

schema_1=StructType([
    StructField("customer",IntegerType(),True),
    StructField("product_model",StringType(),True)
])

data_2=[("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)]

schema_2=StructType([
    StructField("product_model",StringType(),True)
])

def create_session():
    spark = SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()
    return spark

def create_dataframe(spark, data, schema):
    df = spark.createDataFrame(data, schema)
    return df

def find_iphone(df,col_name,product):
    df_new=df.filter(col(col_name)==product)
    return df_new

def upgrade(df1,df2):
    new_df=df1.select("customer").intersect(df2.select("customer"))
    return new_df

def bought_all(df1,df2):
    distinct_models = df2.distinct().count()
    product_bought = df1.groupBy("customer").agg(countDistinct("product_model").alias("product_by_user"))
    result = product_bought.filter((product_bought.product_by_user == distinct_models)).select("customer")
    return result

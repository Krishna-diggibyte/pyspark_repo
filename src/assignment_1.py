from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col
from pyspark.sql.types import StringType,StructType,IntegerType,StructField

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

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

purchase_data_df=spark.createDataFrame(data=data_1,schema=schema_1)
product_data_df=spark.createDataFrame(data=data_2,schema=schema_2)

# purchase_data_df.printSchema()
print("purchase data")
purchase_data_df.show()

# product_data_df.printSchema()
print("product data")
product_data_df.show()

print("2. Find the customers who have bought only iphone13 ")
only_i13_df=purchase_data_df.filter(col("product_model")=="iphone13")
only_i13_df.show()

# print("Find the customers who have bought only iphone14 ")
only_i14_df=purchase_data_df.filter(col("product_model")=="iphone14")
only_i14_df.show()

print("3.Find customers who upgraded from product iphone13 to product iphone14")
i13_to_i14_df=only_i13_df.select("customer").intersect(only_i14_df.select("customer"))
i13_to_i14_df.show()

print("4.Find customers who have bought all models in the new Product Data")

distinct_models=product_data_df.distinct().count()
product_bought=purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("product_by_user"))

result=product_bought.filter((product_bought.product_by_user==distinct_models)).select("customer").show()



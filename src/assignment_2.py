from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,StructType,StructField

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]

schema = StructType([
    StructField("card_number",StringType(),True)
])

print("1.Create a Dataframe as credit_card_df with different read methods")

credit_card_df_direct=spark.createDataFrame(data=data,schema=schema)
credit_card_df_infer=spark.read.csv("../resource/card_number.csv",inferSchema=True,header=True)
credit_card_df_custom=spark.read.csv("../resource/card_number.csv",schema=schema,header=True)

print("Direct read by data and schema")
credit_card_df_direct.show()
# credit_card_df_direct.printSchema()

print("Reading csv file by  infer-schema")
credit_card_df_infer.show()
# credit_card_df_infer.printSchema()

print("Reading csv file by schema")
credit_card_df_custom.show()
# credit_card_df_custom.printSchema()

print("2. print number of partitions")
no_of_partition = credit_card_df_custom.rdd.getNumPartitions()
print("Number of partitions:", no_of_partition)

print("3. Increase the partition size to 5")
increase_partition = credit_card_df_custom.rdd.repartition(no_of_partition+5)
print("New partition: ", increase_partition.getNumPartitions())

print("4. Decrease the partition size back to its original partition size")
decrease_partition = increase_partition.repartition(increase_partition.getNumPartitions() - 5)
print("Updated partition size: ", decrease_partition.getNumPartitions())

print("""5.Create a UDF to print only the last 4 digits marking the remaining digits as *
Eg: ************4567")""")

def masked_card(card_number):
    masked_number = ('*'*(len(card_number)-4))+card_number[-4:]
    return masked_number

credit_mask_udf = udf(masked_card , StringType())

print("6.output should have 2 columns as card_number, masked_card_number")

result_df = credit_card_df_custom.withColumn("masked_card_number" , credit_mask_udf(credit_card_df_custom['card_number']))
result_df.show()

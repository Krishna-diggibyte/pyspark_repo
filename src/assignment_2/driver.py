from src.assignment_2.util import *

spark=create_session()

print("1.Create a Dataframe as credit_card_df with different read methods")
credit_card_df_direct=create_df_direct(spark,data,schema)
credit_card_df_infer=create_df_infer(spark,csv_path)
credit_card_df_custom=create_df_custom(spark,csv_path,schema)

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
no_of_partition = total_partition(credit_card_df_custom)
print("Number of partitions:", no_of_partition)

print("3. Increase the partition size to 5")
increase_partition = inc_partition (credit_card_df_custom,no_of_partition)
print("New partition: ", increase_partition.getNumPartitions())

print("4. Decrease the partition size back to its original partition size")
decrease_partition = dec_partition(increase_partition)
print("Updated partition size: ", decrease_partition.getNumPartitions())

print("""5.Create a UDF to print only the last 4 digits marking the remaining digits as *
Eg: ************4567")""")

print("6.output should have 2 columns as card_number, masked_card_number")

result_df = credit_card_df_custom.withColumn("masked_card_number" , credit_mask_udf(credit_card_df_custom['card_number']))
result_df.show()



from src.assignment_2.util import *
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')


spark=create_session()

logging.info("1.Create a Dataframe as credit_card_df with different read methods")
credit_card_df_direct=create_df_direct(spark,data,schema)
credit_card_df_infer=create_df_infer(spark,csv_path)
credit_card_df_custom=create_df_custom(spark,csv_path,schema)

credit_card_df_direct.show()
logging.info("Direct read by data and schema")
# credit_card_df_direct.printSchema()

credit_card_df_infer.show()
logging.info("Reading csv file by  infer-schema")
# credit_card_df_infer.printSchema()

credit_card_df_custom.show()
logging.info("Reading csv file by schema")
# credit_card_df_custom.printSchema()

no_of_partition = total_partition(credit_card_df_custom)
logging.info("2. print number of partitions")
logging.info(f"Number of partitions: {no_of_partition}")

increase_partition = inc_partition (credit_card_df_custom,no_of_partition)
logging.info("3. Increase the partition size to 5")
inc_num=increase_partition.getNumPartitions()
logging.info(f"New partition: {inc_num}" )

decrease_partition = dec_partition(increase_partition)
logging.info("4. Decrease the partition size back to its original partition size")
dec_num=decrease_partition.getNumPartitions()
logging.info(f"Updated partition size:  {dec_num}" )


logging.info("""5.Create a UDF to print only the last 4 digits marking the remaining digits as * Eg: ************4567")""")

result_df = credit_card_df_custom.withColumn("masked_card_number" , credit_mask_udf(credit_card_df_custom['card_number']))
logging.info("6.output should have 2 columns as card_number, masked_card_number")
result_df.show()



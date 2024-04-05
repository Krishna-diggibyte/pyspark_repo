from src.assignment_1.util import *
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

spark = create_session()

# creating data frame
purchase_data_df= create_dataframe(spark,data_1,schema_1)
product_data_df=create_dataframe(spark,data_2,schema_2)

# purchase_data_df.printSchema()

purchase_data_df.show(truncate=False)
logging.info("Purchase data")
# product_data_df.printSchema()
product_data_df.show(truncate=False)
logging.info("product data")

# "Find the customers who have bought only iphone13 "
only_i13_df=find_iphone(purchase_data_df,"product_model","iphone13")
logging.info("2. Find the customers who have bought only iphone13")
only_i13_df.show()

# "Find the customers who have bought only iphone14 "

only_i14_df=find_iphone(purchase_data_df,"product_model","iphone14")
only_i14_df.show()
logging.info("Find the customers who have bought only iphone14 ")


i13_to_i14_df=upgrade(only_i13_df,only_i14_df)
# i13_to_i14_df=only_i13_df.select("customer").intersect(only_i14_df.select("customer"))
i13_to_i14_df.show()
logging.info("3. Find customers who upgraded from product iphone13 to product iphone14")


result_df=bought_all(purchase_data_df,product_data_df)
result_df.show()
logging.info("4.Find customers who have bought all models in the new Product Data")


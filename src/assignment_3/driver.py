from src.assignment_3.util import *
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')


spark = create_session()

my_dataframe=create_df(spark,data,schema)
logging.info("1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field")
my_dataframe.show()



#renamed columns in the dataframe
new_df = renaming_columns(my_dataframe, new_names)
logging.info("2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function")
new_df.show()

# 3. Write a query to calculate the number of actions performed by each user in the last 7 days
actions_performed = action_count(new_df)
logging.info("3. Write a query to calculate the number of actions performed by each user in the last 7 days")
actions_performed.show()

#Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
result_df = update_column_login(new_df)
logging.info("4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type")
result_df.printSchema()
result_df.show()

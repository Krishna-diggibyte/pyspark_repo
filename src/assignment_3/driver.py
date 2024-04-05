from src.assignment_3.util import *

spark = create_session()

print("1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field")
my_dataframe=create_df(spark,data,schema)
my_dataframe.show()

print("2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function")


#renamed columns in the dataframe
new_df = renaming_columns(my_dataframe, new_names)
new_df.show()

# 3. Write a query to calculate the number of actions performed by each user in the last 7 days
print("3. Write a query to calculate the number of actions performed by each user in the last 7 days")
actions_performed = action_count(new_df)
actions_performed.show()

#Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
print("4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type")
result_df = update_column_login(new_df)
result_df.printSchema()
result_df.show()

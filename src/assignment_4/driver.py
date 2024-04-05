from src.assignment_4.util import *

spark = create_session()

# 1. Read JSON file provided in the attachment using the dynamic function
print("1. Read JSON file provided in the attachment using the dynamic function")

read_df=read_file(spark,path,schema)
read_df.printSchema()
read_df.show()

#2. flatten the data frame which is a custom schema
print("2. flatten the data frame which is a custom schema")

flatted_df=flat_df(read_df)
flatted_df.show()

# 3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)
print("3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)")
print("Before Flatten: ", end="")
print(read_df.count())
print("\nAfter Flatten: ", end="")
print(flatted_df.count())

# 4. Differentiate the difference using explode, explode outer, posexplode functions
print("\n4. Differentiate the difference using explode, explode outer, posexplode functions")
print("explode")
explode_display(read_df)
print("explode_outer")
explode_outer_display(read_df)
print("pos explode")
posexplode_display(read_df)
print("pos explode outer")
posexplode_outer_display(read_df)


# 5. Filter the id which is equal to 0001
print("5. Filter the id which is equal to 0001")
check_id(flatted_df)

# 6. convert the column names from camel case to snake case
print("6. convert the column names from camel case to snake case")

for column in flatted_df.columns:
    flatted_df = flatted_df.withColumnRenamed(column,new=convert_lower(column))

flatted_df.show()

# 7. Add a new column named load_date with the current date
print("7. Add a new column named load_date with the current date")

flat_snake_date_df=add_current_date(flatted_df)
flat_snake_date_df.show()

# 8. create 3 new columns as year, month, and day from the load_date column
print("8. create 3 new columns as year, month, and day from the load_date column")
result_df=add_year_month(flat_snake_date_df)
result_df.show()
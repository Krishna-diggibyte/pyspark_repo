from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_date, year, month, day, posexplode, explode_outer, posexplode_outer
from pyspark.sql.types import StringType, StructType, IntegerType, StructField, ArrayType

spark=SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

# 1. Read JSON file provided in the attachment using the dynamic function
path="../../resource/nested_json_file.json"

schema=StructType([
    StructField("id",IntegerType(),True),
    StructField("properties",StructType([
        StructField("name",StringType(),True),
        StructField("storeSize",StringType(),True)
    ]),True),
    StructField("employees",ArrayType(StructType([
        StructField("empId",IntegerType(),True),
        StructField("empName",StringType(),True)
    ]),True),True)
])
def read_file(path,schema):
    df=spark.read.json(path,schema=schema,multiLine=True)
    return df


read_df=read_file(path,schema)
read_df.printSchema()
read_df.show()

#2. flatten the data frame which is a custom schema
print("2. flatten the data frame which is a custom schema")
temp_flat_df=read_df.withColumn("employee",explode("employees")).drop("employees")


flatted_df=temp_flat_df.withColumn("empId",temp_flat_df.employee.empId).withColumn("empName",temp_flat_df.employee.empName) \
    .withColumn("name",temp_flat_df.properties['name']).withColumn("storeSize",temp_flat_df.properties['storeSize']).drop("employee","properties")

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
read_df.select(read_df.id,read_df.properties,explode(read_df.employees)).show()
print("explode_outer")
read_df.select(read_df.id,read_df.properties,explode_outer(read_df.employees)).show()
print("pos explode")
read_df.select(read_df.id,read_df.properties,posexplode(read_df.employees)).show()
print("pos explode outer")
read_df.select(read_df.id,read_df.properties,posexplode_outer(read_df.employees)).show()




# 5. Filter the id which is equal to 0001
print("5. Filter the id which is equal to 0001")
flatted_df.filter(flatted_df.id == "0001").show()

# 6. convert the column names from camel case to snake case
print("6. convert the column names from camel case to snake case")
def convert_lower(txt):
    new=""
    for i in txt:
        if i.islower():
            new=new+"".join(i)
        else:
            temp=f"_{i}"
            new=new+"".join(temp.lower())
    return new

for column in flatted_df.columns:
    flat_snake_df = flatted_df.withColumnRenamed(column,new=convert_lower(column))

flat_snake_df.show()

# 7. Add a new column named load_date with the current date
print("7. Add a new column named load_date with the current date")

flat_snake_date_df=flat_snake_df.withColumn("load_date",current_date())
flat_snake_date_df.show()

# 8. create 3 new columns as year, month, and day from the load_date column
print("8. create 3 new columns as year, month, and day from the load_date column")
result_df=flat_snake_date_df.withColumn("year",year(flat_snake_date_df["load_date"])).withColumn("month",month(flat_snake_date_df["load_date"])).withColumn("day",day(flat_snake_date_df["load_date"]))
result_df.show()


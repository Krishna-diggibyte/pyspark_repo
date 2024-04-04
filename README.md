# PySpark Assignment

## Question 1 

1. **Form Dataframe:** Formulate the purchase data and product data dataframes as outlined in the queries.
2. **Identify Customers Exclusive to iPhone13 Purchases**: Recognize customers who exclusively bought the "iPhone13" product model.
3. **Determine Customers Upgrading from iPhone13 to iPhone14**: Establish customers who upgraded from the "iPhone13" product model to the "iPhone14" product model.
4. **Locate Customers with Purchases for All Models in New Product Data**: Identify customers who have purchased all product models listed in the new product data.

## Question 2

1. **Initialize SparkSession**: Set up a SparkSession for PySpark utilization.
2. **Method 1: DataFrame Creation via `createDataFrame` Function**: - Utilize the `createDataFrame` function to generate a DataFrame from provided data.
3. **Method 2: CSV File Reading**: - Employ the `credit_cards.csv` function to read credit card data from a CSV file.
4. **Method 3: JSON File Reading**: - Utilize the `credit_cards.json` function to read credit card data from a JSON file.
5. **Partitioning Operations**:
   - Determine the total number of partitions in the DataFrame using `getNumPartitions`.
   - Increase the partition size by 5 partitions using `repartition`.
   - Restore the partition size to its original state.
6. **Masking Credit Card Numbers**:
   - Define a UDF named `masked_card_number` to mask the credit card numbers, revealing only the last 4 digits.
   - Apply the UDF to the DataFrame column containing credit card numbers.
   - Exhibit the DataFrame with masked credit card numbers.

## Question 3:
1. **Column Names Modification:**
   - The DataFrame's column names have dynamically been modified to 'log_id', 'user_id', 'user_activity', and 'time_stamp' through a custom function.
   - The function iteratively renames the existing column names based on the specified new column names.
2. **Action Calculation Query:**
   - A query has been formulated to compute the count of actions performed by each user within the last 7 days.
   - The DataFrame is filtered to encompass only data from the preceding 7 days, then grouped by user_id to tally the action count.
3. **Timestamp Conversion:**
   - The timestamp column has been transformed into a new column titled 'login_date' with the format YYYY-MM-DD and a Date data type.
   - This conversion facilitates simplified handling and analysis of login date information.

## Question 4:
1. **Read JSON File:**
   - The JSON file enclosed in the attachment has been read using a dynamic function, enabling flexibility in reading various JSON file structures.
   - The DataFrame schema is printed and exhibited to comprehend the data structure.
2. **Flatten DataFrame:**
   - The DataFrame has been flattened into a customized schema by leveraging the explode function on nested arrays.
   - The resultant DataFrame encompasses columns for each nested array element, offering a structured view of the data.
3. **Record Count Analysis:**
   - Comparison between the record count before and after flattening the DataFrame has been conducted to discern any disparities.
   - This analysis aids in understanding the impact of flattening on the overall record count.
4. **Explode and PosExplode Functions:**
   - Explode, explode outer, and posexplode functions have been applied to a sample DataFrame to delineate their distinctions.
   - Each function is demonstrated with examples, and the resulting DataFrames are displayed.
5. **Filtering by ID:**
   - Records with a specific ID value (1001) have been filtered from the DataFrame.
   - This filtering operation retrieves specific rows based on the provided condition.
6. **Convert Column Names:**
   - Column names in camel case have been converted to snake case to ensure consistency and readability.
   - A custom function has been executed to effect this conversion, and the DataFrame with updated column names is showcased.
7. **Add Load Date Column:**
   - A fresh column named 'load_date' has been appended to the DataFrame, containing the current date for each record.
   - This column furnishes information about when the data was loaded into the DataFrame.
8. **Create Year, Month, and Day Columns:**
   - From the 'load_date' column, three new columns ('year', 'month', 'day') have been generated to extract the corresponding date components.
   - These columns facilitate further analysis and filtering predicated on specific date attributes.

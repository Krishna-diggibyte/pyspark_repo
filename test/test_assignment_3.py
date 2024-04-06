import unittest
from src.assignment_3.util import *


class TestAssignment3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark =SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_renaming_columns(self):
        df = create_df(self.spark, data, schema)
        expected_df = renaming_columns(df,new_names)
        test_log_data = [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout', '2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]
        test_log_schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("user_activity", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        actual_df = create_df(self.spark, test_log_data, test_log_schema)
        self.assertEqual(expected_df.collect(), actual_df.collect())

    def test_action_count(self):
        df = create_df(self.spark,data, schema)
        df = renaming_columns(df,new_names)
        expected_df = action_count(df)
        self.assertEqual(expected_df.count(), 3)

    def test_update_column_login(self):
        df = create_df(self.spark, data, schema)
        df = renaming_columns(df,new_names)
        expected_df = update_column_login(df)
        test_data = [
            (1, 101, 'login', '2023-09-05'),
            (2, 102, 'click', '2023-09-06'),
            (3, 101, 'click', '2023-09-07'),
            (4, 103, 'login', '2023-09-08'),
            (5, 102, 'logout', '2023-09-09'),
            (6, 101, 'click', '2023-09-10'),
            (7, 103, 'click', '2023-09-11'),
            (8, 102, 'click', '2023-09-12')
        ]
        test_schema = ["log_id", "user_id", "user_activity", "timestamp"]
        actual_df = create_df(self.spark, test_data, test_schema)
        actual_df = renaming_columns(actual_df,new_names)
        actual_df = actual_df.select("log_id", "user_id", "user_activity", to_date("time_stamp").alias("login_date"))
        self.assertEqual(expected_df.collect(), actual_df.collect())
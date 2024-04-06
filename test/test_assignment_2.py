import unittest

from pyspark.sql.types import LongType

from src.assignment_2.util import *


class TestAssignment2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("Krishna").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_create_df_direct(self):
        test_data = [("1234567891234567",),
                ("5678912345671234",),
                ("9123456712345678",),
                ("1234567812341122",),
                ("1234567812341342",)]

        test_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        df = create_df_direct(self.spark, test_data, test_schema)
        self.assertEqual(df.count(), len(test_data))

    def test_create_df_infer(self):
        csv_path = "../resource/card_number.csv"
        test_credit_card_df_infer=create_df_infer(self.spark,csv_path)
        expected_test_data = [(1234567891234567,),
                              (5678912345671234,),
                              (9123456712345678,),
                              (1234567812341122,),
                              (1234567812341342,)]

        test_schema = StructType([
            StructField("card_number", LongType(), True)
        ])
        expected_df=create_df_direct(self.spark,expected_test_data,test_schema)
        self.assertEqual(test_credit_card_df_infer.collect(),expected_df.collect())



    def test_create_df_custom(self):
        csv_path="../resource/card_number.csv"
        test_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        test_custom_df = create_df_custom(self.spark, csv_path, test_schema)
        expected_data = [("1234567891234567",),
                                 ("5678912345671234",),
                                 ("9123456712345678",),
                                 ("1234567812341122",),
                                 ("1234567812341342",)]
        expected_custom_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        expected_df = create_df_direct(self.spark, expected_data, expected_custom_schema)
        self.assertEqual(test_custom_df.collect(), expected_df.collect())



    def test_total_partitions(self):
        csv_path = "../resource/card_number.csv"
        test_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        df = create_df_custom(self.spark, csv_path, test_schema)
        partitions = total_partition(df)
        self.assertEqual(partitions, 1)

    def test_inc_partition(self):
        csv_path = "../resource/card_number.csv"
        test_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        df = create_df_custom(self.spark, csv_path, test_schema)
        initial_partitions = total_partition(df)
        new_partitions = inc_partition(df,initial_partitions)
        self.assertEqual(new_partitions.getNumPartitions(), initial_partitions + 5)

    def test_dec_partition(self):
        csv_path = "../resource/card_number.csv"
        test_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        df = create_df_custom(self.spark, csv_path, test_schema)
        initial_partitions = total_partition(df)
        new_partitions = inc_partition(df, initial_partitions)
        final_partitions = dec_partition(new_partitions)
        self.assertEqual(final_partitions.getNumPartitions(), 1)

    def test_masked_card(self):
        csv_path = "../resource/card_number.csv"
        test_schema = StructType([
            StructField("card_number", StringType(), True)
        ])
        df1 = create_df_custom(self.spark, csv_path, test_schema)

        df = df1.withColumn("masked_number", credit_mask_udf(df1["card_number"]))

        expected_masked_numbers = ['************4567', '************1234', '************5678',
                                   '************1122', '************1342']
        masked_numbers = df.select("masked_number").rdd.flatMap(lambda x: x).collect()

        self.assertEqual(masked_numbers, expected_masked_numbers)


if __name__ == '__main__':

    unittest.main()
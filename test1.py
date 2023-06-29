import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

def etl_function():
    spark = SparkSession.builder\
        .appName("ETL")\
        .config("spark.jars", "/Users/shiva/Desktop/assignment/postgresql-42.2.6.jar") \
        .getOrCreate()

    # extract data
    df = spark.read.format("csv").option("header", "true").load('file:///Users/shiva/Desktop/assignment/sales.csv')
   
    df = df.filter((df.Orderno != ''))
    df = df.filter(df.Quantityorder != '')
    df = df.filter(df.Priceforone >=600 )
    df = df.filter(df.date >=01/01/2011 )
    df = df.withColumn("date", to_date(df["date"], "dd/mm/yyyy"))
    df = df.withColumn("Id",df.Id.cast('bigint'))
    df = df.withColumn("Age",df.Orderno.cast('int'))
    df = df.withColumn("marks",df.Quantityorder.cast('int'))
    df = df.withColumn("marks",df.Priceforone.cast('int'))

    result_df.write\
    .format("jdbc")\
    .mode("append")\
    .option("url", "jdbc:postgresql://localhost:5434/assignment")\
    .option("dbtable", "student_test")\
    .option("user", "postgres")\
    .option("password", "password")\
    .option("driver", "org.postgresql.Driver")\
    .save()
    result_df.show()

    spark.stop()

if __name__ == "__main__":
    etl_function()
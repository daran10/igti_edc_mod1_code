from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Exercise")
    .getOrCreate()
)

# ler os dados de enem de 2019

enem = ( 
    spark
    .read  
    .format('csv') \
    .option('delimiter', ';') \
    .option('inferschema', True) \
    .load("C:\\Users\\daran\\Documents\\dados\\MICRODADOS_ENEM_2019.csv", header = True)
)

(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year")
    .save("C:\\Users\\daran\\Documents\\dados\\staging\\enem")
)

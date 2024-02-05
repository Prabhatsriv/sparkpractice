from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParquetRead_AvroWrite").master("local[*]")\
         .config("spark.jars","file:///C:/Users/PRABHAT/Downloads/spark-avro_2.11-2.4.3.jar") \
    .getOrCreate()


data = [("ram","ayodha",40),("sita","janakpuri",35)]
df = spark.createDataFrame(data).toDF("name","location","age")
df.repartition(1).write.format("com.databricks.spark.avro")\
             .mode("overwrite")\
             .save("outAvro")

print("avro write successful")

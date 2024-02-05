from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("parquetSchema").master("local[*]").getOrCreate()
print(spark)

data = [("prabhat",33,"golghar"),
        ("amit",32,"baraipara"),
        ("abhishek",33,"purani bazar")]

df = spark.createDataFrame(data).toDF("name","age","location")
df.show()
df.printSchema()

print("write parquet file to folder")
df.repartition(1).write.format("parquet").mode("overwrite").save("outParquet")
print("write succesful")

df_schema = df.schema.json()
print(df_schema)

print("read parquet file from folder")
inParquetDf = spark.read.format("parquet").load("outParquet")
inParquetDf.show()
inParquetDf.printSchema()
print("read succesful")

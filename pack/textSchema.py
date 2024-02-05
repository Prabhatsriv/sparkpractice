from pyspark.sql import SparkSession
from pyspark.sql.types import _parse_datatype_string


spark = SparkSession.builder.appName("textSchema") \
    .master("local[*]").getOrCreate()
print("Pyspark  session is ready")
print(spark)

print('------------------')
f = open('schema_file.txt','r')


schema_str = str(f.read())
print('new schema from file ' , schema_str)
print(type(schema_str))


schema = _parse_datatype_string(schema_str)
print(schema)

data = [(1,"prabhat"),(2,"abhishek")]

df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

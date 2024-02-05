from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json

spark = SparkSession.builder.appName("jsonSchema")\
             .master("local[*]").getOrCreate()
print("Pyspark session is ready")
print(spark)


data = [(1,"prabhat"),(2,"abhishek")]

df = spark.createDataFrame(data).toDF("id","name")
df.show()
df.printSchema()

print("---------- Dataframe Schema <> JSON")
#save schema from the original DataFrame into json:
schema_json = df.schema.json()
print(schema_json)
print("saving the retrived json schema into file")
with open("json_schema.json","w") as j:
    json.dump(schema_json,j)
print("write successful")

#restore schema from json
new_schema = StructType.fromJson(json.loads(schema_json))
print(new_schema)


print("printing DataFrame2 with new schema")
data2 = [(3,"anurag"),(4,"saddam")]
df2 = spark.createDataFrame(data2, new_schema)
df2.show()
df2.printSchema()

#reading new DataFrame3 with json schema read from exteral json file
print("reading json schema from external file")
with open("json_schema.json","r") as jo:
    read_json_schema = json.load(jo)

print(read_json_schema)
print(type(read_json_schema))

print("covnert json schema into DataFrame Shchema(DataType)")
json_schema = StructType.fromJson(json.loads(read_json_schema))

data3 = [(5,"ritanshu"),(6,"rakesh")]
df3 = spark.createDataFrame(data3, json_schema)
df3.show()
df3.printSchema()

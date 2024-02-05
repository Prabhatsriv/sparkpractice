from pyspark.sql import SparkSession

def renameCol(l:list) -> list:
    l1 = []
    for i in l:
        l1.append(i+"_new")
    return l1

spark = SparkSession.builder.appName("renameMultiCol").master("local[*]").getOrCreate()

column1 = ["name","age","location"]
print(type(column1))
data = [("prabhat",33,"golghar"),("amit",32,"baraipara"),("abhishek",33,"purani bazar")]
rdd = spark.sparkContext.parallelize(data)
df1 = spark.createDataFrame(rdd,column1)
df1.show()
df1.printSchema()
column2 = df1.columns
#print(column2)
#print(type(column2))
column3 = renameCol(column2)
#print(column3)
#print(type(column3))
selected_col = ["name_new","age_new"]

df3 = spark.createDataFrame(rdd,column3)\
           .select(*selected_col)
df3.show()
df3.printSchema()
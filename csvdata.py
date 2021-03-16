import pyspark.sql
from pyspark.sql import functions as F

spark = pyspark.sql.SparkSession.builder.master("local[*]").appName("csvdata").getOrCreate()
data = "F:\\bigdata\\Dataset\\us-500.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(data)
df.createTempView("tab")
res = spark.sql("select * from tab where state = 'NJ'")
res.show()
res = spark.sql("select state, count(*) cnt from tab group by state order by cnt desc")
res.show()
res = df.where((F.col("state")=="NJ") & (F.col("email").like("%gmail.com%")))
res.show()
res = df.groupBy(F.col("state")).count().orderBy(F.col("count"), ascending=False)
res.show()

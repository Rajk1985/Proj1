import pyspark.sql
from pyspark.sql import functions as F

spark = pyspark.sql.SparkSession.builder.master("local[*]").config("spark.jar","F:\bigdata\Softwares\drivers").appName("csvdata").getOrCreate()

url ="jdbc:oracle:thin://localhost:1521/orcl"
df = spark.read.format("jdbc").option("url",url).option("user","scott").option("password","tiger").option("driver","oracle.jdbc.OracleDriver").option("dbtable","DEPT").load()
df.show()
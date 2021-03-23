import pyspark.sql
from pyspark.sql import functions as F

spark = pyspark.sql.SparkSession.builder.master("local[*]").config("spark.jar","F:\bigdata\Softwares\drivers").appName("csvdata").getOrCreate()

url ="jdbc:oracle:thin://localhost:1521/orcl"
df = spark.read.format("jdbc").option("url",url).option("user","scott").option("password","tiger").option("driver","oracle.jdbc.OracleDriver").option("dbtable","DEPT").load()
df.show()

#jars="F:\bigdata\Softwares\drivers"
#spark = pyspark.sql.SparkSession.builder.master("local[*]").config("spark.driver.extraClassPath",jars).config("spark.executor.extraClassPath",jars).appName("csvdata").getOrCreate()
#url = "jdbc:mysql://mysqldb.cmpuk2t3n5oa.ap-south-1.rds.amazonaws.com:3306/mysqldb"
#df = spark.read.format("jdbc").option("url",url).option("user","musername").option("password","mpassword").option("driver","com.mysql.jdbc.Driver").option("dbtable","my10krecord").load()
#df.show()
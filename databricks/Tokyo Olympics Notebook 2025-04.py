# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="my_scope",key="secret1")

spark.conf.set("fs.azure.account.auth.type.olympicsdata187.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.olympicsdata187.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.olympicsdata187.dfs.core.windows.net", "5e44340e-9ec9-42e1-a485-a5009c2e7936")
spark.conf.set("fs.azure.account.oauth2.client.secret.olympicsdata187.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.olympicsdata187.dfs.core.windows.net", "https://login.microsoftonline.com/e7d5eae7-62d8-4172-bccf-974dd57f35b9/oauth2/v2.0/token")



# COMMAND ----------

spark.conf.set("fs.azure.account.key.olympicsdata187.dfs.core.windows.net", "dyc7kC0lwboJqo9CrP3OD9ayz4GbXMwr/HbAhuzzuodY+GNTPrq3BUp+VeWjR/kyWW4x4CleFEtG+AStFjmlrQ==")

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Olympics_Data_Processing") \
    .config("fs.azure.account.key.olympicsdata187.dfs.core.windows.net", "WYbLwt/mK/hHhnOxFSerptMPjb7NL/lTlmX5dOwP7PNRaJsOZ1bTNzREb788xRgCkrhNkEejk7yb+ASt3VrO+A==") \
    .getOrCreate()

athletes = spark.read.format("csv").option("header","true").load("abfss://tokyo-olympic-data@olympicsdata187.dfs.core.windows.net/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").load("abfss://tokyo-olympic-data@olympicsdata187.dfs.core.windows.net/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").load("abfss://tokyo-olympic-data@olympicsdata187.dfs.core.windows.net/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").load("abfss://tokyo-olympic-data@olympicsdata187.dfs.core.windows.net/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").load("abfss://tokyo-olympic-data@olympicsdata187.dfs.core.windows.net/raw-data/teams.csv")

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

entriesgender.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

entriesgender = entriesgender.select(
    [col(c.strip()) for c in entriesgender.columns])
entriesgender = entriesgender.withColumn("Female", trim(col("Female")).cast(IntegerType()))\
.withColumn("Male", trim(col("Male")).cast(IntegerType()))\
.withColumn("Total", trim(col("Total")).cast(IntegerType()))




# COMMAND ----------



# COMMAND ----------

top_gold_medal_countries= medals.orderBy("Gold", ascending=False).select("Team_Country", "Gold").show()

# COMMAND ----------

athletes.write.option("header","true").mode("overwrite").parquet("abfss://tokyo-olympic-data@olympicsdata187.dfs.core.windows.net/processed-data/athletes")
medals.write.option("header","true").mode("overwrite").parquet("abfss://tokyo-olympic-data@olympicsdata187.dfs.core.windows.net/processed-data/medals")
teams.write.option("header","true").mode("overwrite").parquet("abfss://tokyo-olympic-data@olympicsdata187.dfs.core.windows.net/processed-data/teams")
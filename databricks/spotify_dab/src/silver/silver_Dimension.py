# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

sa     = "storageaccspotify"
bronze = f"abfss://bronze@{sa}.dfs.core.windows.net"
silver = f"abfss://silver@{sa}.dfs.core.windows.net"
catalog_schema = "spotify_catalog.silver"



# preview: runs the Auto Loader stream into a scratch folder and shows the result as a table
def preview(df, name):
    for s in spark.streams.active:          # stop leftover streams first
        s.stop()
    base = f"{silver}/_preview/{name}"
    dbutils.fs.rm(base, True)

    q = df.writeStream.format("delta")\
          .outputMode("append")\
          .option("checkpointLocation", f"{base}/chk")\
          .trigger(availableNow=True)\
          .start(f"{base}/data")
    q.awaitTermination()

    display(spark.read.format("delta").load(f"{base}/data"))

# COMMAND ----------

import os
import sys

project_pth = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_pth)

from utils.transformations import reusable


# COMMAND ----------

# MAGIC %md
# MAGIC ##DimUser

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation",f"{silver}/DimUser/schema")\
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
            .load(f"{bronze}/DimUser")

# COMMAND ----------

preview(df_user, "DimUser")

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))
preview(df_user, "DimUser")

# COMMAND ----------

df_user_obj =  reusable()
df_user = df_user_obj.dropColumns(df_user, ["_rescued_data"])
df_user = df_user_obj.dropDuplicates(df_user,["user_id"]) 
preview(df_user, "DimUser")

# COMMAND ----------

#display(spark.sql("SHOW TABLES IN spotify_catalog.silver"))

# COMMAND ----------

#spark.sql("DROP TABLE IF EXISTS spotify_catalog.silver.dim_user")

# COMMAND ----------

#dbutils.fs.rm(f"{silver}/DimUser/data", True)
#dbutils.fs.rm(f"{silver}/DimUser/checkpoint", True)

# COMMAND ----------

#display(spark.sql("SHOW TABLES IN spotify_catalog.silver"))

# COMMAND ----------

q = df_user.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation",f"{silver}/DimUser/checkpoint")\
            .trigger(availableNow=True)\
            .option("path",f"{silver}/DimUser/data")\
            .toTable(f"{catalog_schema}.DimUser")
q.awaitTermination()

# COMMAND ----------

#display(spark.sql("SHOW TABLES IN spotify_catalog.silver"))
#display(spark.table(f"{catalog_schema}.DimUser"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##DimArtist

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation",f"{silver}/DimArtist/schema")\
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
            .load(f"{bronze}/DimArtist")

# COMMAND ----------

preview(df_art, "DimArtist")

# COMMAND ----------

df_art = reusable().dropColumns(df_art, ['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])
preview(df_art, "DimArtist")

# COMMAND ----------

q = df_art.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation",f"{silver}/DimArtist/checkpoint")\
            .trigger(availableNow=True)\
            .option("path",f"{silver}/DimArtist/data")\
            .toTable(f"{catalog_schema}.DimArtist")
q.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ##DimTrack

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation",f"{silver}/DimTrack/schema")\
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
            .load(f"{bronze}/DimTrack")

# COMMAND ----------

preview(df_track, "DimTrack")

# COMMAND ----------

df_track = df_track.withColumn("durationFlag", when(col('duration_sec') < 150, "low")\
                                            .when(col('duration_sec') > 300, "medium")\
                                            .otherwise("high"))

df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"),"-"," "))
df_track = reusable().dropColumns(df_track, ['_rescued_data'])

preview(df_track, "DimTrack")

# COMMAND ----------

q = df_track.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation",f"{silver}/DimTrack/checkpoint")\
            .trigger(availableNow=True)\
            .option("path",f"{silver}/DimTrack/data")\
            .toTable(f"{catalog_schema}.DimTrack")
q.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ##DimDate

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation",f"{silver}/DimDate/schema")\
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
            .load(f"{bronze}/DimDate")

# COMMAND ----------

preview(df_date, "DimDate")

# COMMAND ----------

df_date = reusable().dropColumns(df_date, ['_rescued_data'])

q = df_date.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation",f"{silver}/DimDate/checkpoint")\
            .trigger(availableNow=True)\
            .option("path",f"{silver}/DimDate/data")\
            .toTable(f"{catalog_schema}.DimDate")
q.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ##FactStream

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format","parquet")\
            .option("cloudFiles.schemaLocation",f"{silver}/FactStream/schema")\
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
            .load(f"{bronze}/FactStream")

# COMMAND ----------

preview(df_fact, "FactStream")

# COMMAND ----------

df_fact = reusable().dropColumns(df_fact, ['_rescued_data'])

q = df_fact.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation",f"{silver}/FactStream/checkpoint")\
            .trigger(availableNow=True)\
            .option("path",f"{silver}/FactStream/data")\
            .toTable(f"{catalog_schema}.FactStream")
q.awaitTermination()

# COMMAND ----------


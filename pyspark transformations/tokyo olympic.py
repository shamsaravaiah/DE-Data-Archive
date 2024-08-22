# Databricks notebook source
 # Replace with your storage account name and access key
storage_account_name = "tokyoolympicdatasham"
storage_account_access_key = "87xgvNVmcTKfFAlq71GTfn+IfYsnAGzzrRVpI1+yXGHxsRVlS9MSLT3P1Ug9JLuaYQr3hWU5pGMl+ASt3FVpGA=="

container_name = "tokyo-olympic-data-container" 
mount_point = "/mnt/tokyo_olympics_data"


# Mount the storage account to Databricks
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = mount_point,
  extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# COMMAND ----------

files = dbutils.fs.ls("/mnt/tokyo_olympics_data")
display(files)


# COMMAND ----------

files_in_rawdata = dbutils.fs.ls("/mnt/tokyo_olympics_data/raw-data")
display(files_in_rawdata)

# COMMAND ----------

coaches_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyo_olympics_data/raw-data/2425b2ca-d976-400e-89ff-b6a491d1d1f9")
teams_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyo_olympics_data/raw-data/2af96f1d-4f04-4b82-84be-459b1c5c9c28")
athletes_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyo_olympics_data/raw-data/6fa92868-570d-41ba-bc6c-ac14d1dd59ea")
EntriesGender_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyo_olympics_data/raw-data/96af8465-23af-4ec7-93e6-b94b09a45d22")
Medals_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyo_olympics_data/raw-data/afbd3984-b951-4232-b085-f71a814fcba4")


# COMMAND ----------

print("coaches schema")
(coaches_df.printSchema())
print("teams schema")
(teams_df.printSchema())
print("athletes schema")
(athletes_df.printSchema())
print("EntriesGender schema")
(EntriesGender_df.printSchema())
print("Medals schema")
(Medals_df.printSchema())



# COMMAND ----------

# MAGIC %md
# MAGIC JOINING DATA FRAMES  <br />
# MAGIC
# MAGIC Join coaches_df with teams_df  <br />
# MAGIC athletes_df with teams_df  <br />
# MAGIC EntriesGender_df with coaches_df  <br />
# MAGIC Medals_df with teams_df 

# COMMAND ----------


joined_df = coaches_df.join(teams_df, ["Discipline", "Event", "Country"], "inner")
athletes_teams_df = athletes_df.join(teams_df, ["Discipline", "Country"], "inner")
coaches_gender_df = coaches_df.join(EntriesGender_df, "Discipline", "left")
medals_teams_df = Medals_df.join(teams_df, Medals_df.TeamCountry == teams_df.Country, "inner")


# COMMAND ----------

# MAGIC %md 
# MAGIC AGREGATTING DATA FRAMES  <br />
# MAGIC Total Medals by Country: <br />
# MAGIC Count of Coaches by Discipline:  <br />
# MAGIC Average Gender Distribution in Entries:  <br />
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg, sum, countDistinct


#total medals by country
total_medals_by_country = Medals_df.groupBy("TeamCountry").agg(
    sum("Gold").alias("TotalGold"),
    sum("Silver").alias("TotalSilver"),
    sum("Bronze").alias("TotalBronze"),
    sum("Total").alias("TotalMedals")
)

#Count of Coaches by Discipline
coaches_count = coaches_df.groupBy("Discipline").count()


# Average Gender Distribution in Entries
avg_gender_distribution = EntriesGender_df.groupBy("Discipline").agg(
    avg("Female").alias("AvgFemale"),
    avg("Male").alias("AvgMale"),
    avg("Total").alias("AvgTotal")
)



# COMMAND ----------

# MAGIC %md 
# MAGIC FILTERING DATAFRAMES  <br />
# MAGIC Filter Athletes by Discipline:  <br /> 
# MAGIC Select Specific Columns:  <br /> 
# MAGIC
# MAGIC Data Enrichment  <br /> 
# MAGIC Add Total Medals Column:  <br /> 
# MAGIC Create a Discipline Summary:  <br /> 
# MAGIC
# MAGIC  Data Transformation <br /> 
# MAGIC  Convert Column Types: <br />
# MAGIC  Calculate Rank Difference: <br />
# MAGIC  

# COMMAND ----------

#Filtering Athletes by Discipline
athletes_filtered = athletes_df.filter(athletes_df.Discipline == "Swimming")
# Selectes Specific Columns within a medals dataframe
selected_columns = Medals_df.select("TeamCountry", "Gold", "Silver", "Bronze")


# COMMAND ----------

#Add Total Medals Column
enriched_medals_df = Medals_df.withColumn("TotalMedals", Medals_df["Gold"] + Medals_df["Silver"] + Medals_df["Bronze"])

# Create a Discipline Summary
discipline_summary = teams_df.groupBy("Discipline").agg(
    countDistinct("TeamName").alias("NumTeams"),
    countDistinct("Country").alias("NumCountries")
)


# COMMAND ----------

from pyspark.sql.functions import col


# Converting Column Types
transformed_df = EntriesGender_df.withColumn("Female", col("Female").cast("integer"))


# Calculating Rank Difference
medals_with_diff = Medals_df.withColumn("RankDifference", col("Rank by Total") - col("Rank"))


# COMMAND ----------

# MAGIC %md
# MAGIC Writing back to Data lake storage
# MAGIC

# COMMAND ----------

base_path = "/mnt/tokyo_olympics_data/transformed-data/"

# Writing each DataFrame to its respective path in Parquet format

total_medals_by_country.write.mode("overwrite").parquet(f"{base_path}/total_medals_by_country")
coaches_count.write.mode("overwrite").parquet(f"{base_path}/coaches_count")
avg_gender_distribution.write.mode("overwrite").parquet(f"{base_path}/avg_gender_distribution")
athletes_filtered.write.mode("overwrite").parquet(f"{base_path}/athletes_filtered")
selected_columns.write.mode("overwrite").parquet(f"{base_path}/selected_columns")
enriched_medals_df.write.mode("overwrite").parquet(f"{base_path}/enriched_medals_df")
discipline_summary.write.mode("overwrite").parquet(f"{base_path}/discipline_summary")
transformed_df.write.mode("overwrite").parquet(f"{base_path}/transformed_df")
medals_with_diff.write.mode("overwrite").parquet(f"{base_path}/medals_with_diff")



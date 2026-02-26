# Databricks notebook source
# MAGIC %md
# MAGIC # Data provisioning
# MAGIC
# MAGIC Data is uploaded manually to the file store. 
# MAGIC The paths are:
# MAGIC - stations.csv = /FileStore/tables/stations.csv
# MAGIC - riders.csv = /FileStore/tables/riders.csv
# MAGIC - payments.csv = /FileStore/tables/payments.csv
# MAGIC - trips.csv = /FileStore/tables/trips.csv
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Create bronze schema
# MAGIC Create bronze schemas for raw data ingestion.
# MAGIC

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# MAGIC %md
# MAGIC # Read files
# MAGIC Files were manually uploaded to FileStore

# COMMAND ----------

stations_df = (spark.read.option("header", False).option("inferSchema", True)
               .csv("/FileStore/tables/stations.csv"))
riders_df   = (spark.read.option("header", False).option("inferSchema", True)
               .csv("/FileStore/tables/riders.csv"))
payments_df = (spark.read.option("header", False).option("inferSchema", True)
               .csv("/FileStore/tables/payments.csv"))
trips_df    = (spark.read.option("header", False).option("inferSchema", True)
               .csv("/FileStore/tables/trips.csv"))

# COMMAND ----------

display(stations_df)
display(riders_df)
display(payments_df)
display(trips_df)

# COMMAND ----------

stations_df.write.format("delta").mode("overwrite").saveAsTable("bronze.stations")
riders_df.write.format("delta").mode("overwrite").saveAsTable("bronze.riders")
payments_df.write.format("delta").mode("overwrite").saveAsTable("bronze.payments")
trips_df.write.format("delta").mode("overwrite").saveAsTable("bronze.trips")

# COMMAND ----------

spark.sql("SELECT COUNT(*) AS count_stations FROM bronze.stations").show()
spark.sql("SELECT COUNT(*) AS count_riders FROM bronze.riders").show()
spark.sql("SELECT COUNT(*) AS count_payments FROM bronze.payments").show()
spark.sql("SELECT COUNT(*) AS count_trips FROM bronze.trips").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Stage
# MAGIC In this stage, raw Bronze tables were standardized into clean Silver tables for analytics-ready transformations.
# MAGIC
# MAGIC ## Transformations
# MAGIC 1. Renamed generic columns (_c0, _c1, …) to business column names based on the dataset schema.
# MAGIC 2. Applied relevant type casting
# MAGIC 3. Derived trip metrics, e.g. `trip_duration_minutes` from `ended_at` and `started_at`
# MAGIC 4. Applied basic quality filters, e.g. removed rows with null required keys
# MAGIC
# MAGIC ## Outputs
# MAGIC Cleaned outputs were witten to:
# MAGIC `silver.stations`
# MAGIC `silver.riders`
# MAGIC `silver.payments`
# MAGIC `silver.trips`

# COMMAND ----------

from pyspark.sql import functions as F

# Ensure schema exists 
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# -------------------------
# stations
# -------------------------
stations_silver = (
    spark.table("bronze.stations")
    .select(
        F.col("_c0").cast("string").alias("station_id"),
        F.col("_c1").cast("string").alias("name"),
        F.col("_c2").cast("double").alias("latitude"),
        F.col("_c3").cast("double").alias("longitude")
    )
    .filter(F.col("station_id").isNotNull())
)

# write
stations_silver.write.format("delta").mode("overwrite").saveAsTable("silver.stations")

# -------------------------
# riders
# -------------------------
riders_silver = (
    spark.table("bronze.riders")
    .select(
        F.col("_c0").cast("int").alias("rider_id"),
        F.col("_c1").cast("string").alias("first"),
        F.col("_c2").cast("string").alias("last"),
        F.col("_c3").cast("string").alias("address"),
        F.to_date(F.col("_c4")).alias("birthday"),
        F.to_date(F.col("_c5")).alias("account_start_date"),
        F.to_date(F.col("_c6")).alias("account_end_date"),
        F.col("_c7").cast("boolean").alias("is_member")
    )
    .filter(F.col("rider_id").isNotNull())
)

# write 
riders_silver.write.format("delta").mode("overwrite").saveAsTable("silver.riders")

# -------------------------
# payments
# -------------------------
payments_silver = (
    spark.table("bronze.payments")
    .select(
        F.col("_c0").cast("int").alias("payment_id"),
        F.to_date(F.col("_c1")).alias("payment_date"),
        F.col("_c2").cast("decimal(10,2)").alias("amount"),
        F.col("_c3").cast("int").alias("rider_id")
    )
    .filter(F.col("payment_id").isNotNull() & F.col("rider_id").isNotNull())
)

# write
payments_silver.write.format("delta").mode("overwrite").saveAsTable("silver.payments")

# -------------------------
# trips
# -------------------------
trips_silver = (
    spark.table("bronze.trips")
    .select(
        F.col("_c0").cast("string").alias("trip_id"),
        F.col("_c1").cast("string").alias("rideable_type"),
        F.to_timestamp(F.col("_c2")).alias("started_at"),
        F.to_timestamp(F.col("_c3")).alias("ended_at"),
        F.col("_c4").cast("string").alias("start_station_id"),
        F.col("_c5").cast("string").alias("end_station_id"),
        F.col("_c6").cast("int").alias("rider_id")
    )
    .withColumn(
        "trip_duration_minutes",
        (F.col("ended_at").cast("long") - F.col("started_at").cast("long")) / F.lit(60.0)
    )
    .filter(
        F.col("trip_id").isNotNull() &
        F.col("started_at").isNotNull() &
        F.col("ended_at").isNotNull() &
        F.col("rider_id").isNotNull()
    )
)

# write 
trips_silver.write.format("delta").mode("overwrite").saveAsTable("silver.trips")


# COMMAND ----------

# Checks 
display(
    spark.createDataFrame([
        ("silver.stations", spark.table("silver.stations").count()),
        ("silver.riders",   spark.table("silver.riders").count()),
        ("silver.payments", spark.table("silver.payments").count()),
        ("silver.trips",    spark.table("silver.trips").count())
    ], ["table_name", "row_count"])
)


# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Stage
# MAGIC Data is prepared for analytics.
# MAGIC
# MAGIC - Dimension tables
# MAGIC   - gold.dim_rider
# MAGIC   - gold.dim_station
# MAGIC   - gold.dim_date
# MAGIC
# MAGIC - Fact tables
# MAGIC   - gold.fact_trip
# MAGIC   - gold.fact_payment
# MAGIC
# MAGIC ### Transformations
# MAGIC 1. Built shared dimensions for riders, stations and dates.
# MAGIC 2. Created `fact_trips` with `trip_duration_minutes`, `rider_age_at_trip`, where each row represents a trip
# MAGIC 3. Created `fact_payments` with `payment_amount`
# MAGIC
# MAGIC ### Output
# MAGIC The tables were saved as delta tables.
# MAGIC
# MAGIC

# COMMAND ----------

# Create schema if not already existing
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# Build dimensions
from pyspark.sql import functions as F

#### Riders 
dim_rider = (
    spark.table("silver.riders")
    .select(
        F.col("rider_id"),
        F.col("first"),
        F.col("last"),
        F.col("birthday"),
        F.col("account_start_date"),
        F.col("account_end_date"),
        F.col("is_member")
    )
    .dropDuplicates(["rider_id"])
)

dim_rider.write.format("delta").mode("overwrite").saveAsTable("gold.dim_rider")

### Stations
dim_station = (
    spark.table("silver.stations")
    .select("station_id", "name", "latitude", "longitude")
    .dropDuplicates(["station_id"])
)

# write 
dim_station.write.format("delta").mode("overwrite").saveAsTable("gold.dim_station")

### Trips 
trip_dates = (
    spark.table("silver.trips")
    .select(F.to_date("started_at").alias("date"))
    .union(spark.table("silver.trips").select(F.to_date("ended_at").alias("date")))
)

payment_dates = spark.table("silver.payments").select(F.col("payment_date").alias("date"))

all_dates = trip_dates.union(payment_dates).filter(F.col("date").isNotNull()).dropDuplicates()


### Dates 
dim_date = (
    all_dates
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .select("date_key", "date", "year", "quarter", "month", "day", "day_of_week")
)

# write
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")

# COMMAND ----------

# Build fact tables with trip_duration_minutes and rider_age_at_trip
trips = spark.table("silver.trips")
riders = spark.table("gold.dim_rider")

fact_trip = (
    trips.alias("t")
    .join(riders.alias("r"), F.col("t.rider_id") == F.col("r.rider_id"), "left")
    .withColumn("start_date", F.to_date("t.started_at"))
    .withColumn("end_date", F.to_date("t.ended_at"))
    .withColumn("start_date_key", F.date_format("start_date", "yyyyMMdd").cast("int"))
    .withColumn("end_date_key", F.date_format("end_date", "yyyyMMdd").cast("int"))
    .withColumn(
        "rider_age_at_trip",
        F.floor(F.months_between(F.col("t.started_at"), F.col("r.birthday")) / 12)
    )
    .select(
        F.col("t.trip_id"),
        F.col("t.rider_id"),
        F.col("t.start_station_id"),
        F.col("t.end_station_id"),
        F.col("start_date_key"),
        F.col("end_date_key"),
        F.col("t.rideable_type"),
        F.col("t.trip_duration_minutes"),
        F.col("rider_age_at_trip")
    )
)

fact_trip.write.format("delta").mode("overwrite").saveAsTable("gold.fact_trip")


# COMMAND ----------

# fact_payment (includes required payment amount)

payments = spark.table("silver.payments")

fact_payment = (
    payments
    .withColumn("payment_date_key", F.date_format("payment_date", "yyyyMMdd").cast("int"))
    .select(
        "payment_id",
        "rider_id",
        "payment_date_key",
        F.col("amount").alias("payment_amount")
    )
)

fact_payment.write.format("delta").mode("overwrite").saveAsTable("gold.fact_payment")


# COMMAND ----------

# Check 
display(
    spark.createDataFrame([
        ("gold.dim_rider",   spark.table("gold.dim_rider").count()),
        ("gold.dim_station", spark.table("gold.dim_station").count()),
        ("gold.dim_date",    spark.table("gold.dim_date").count()),
        ("gold.fact_trip",   spark.table("gold.fact_trip").count()),
        ("gold.fact_payment",spark.table("gold.fact_payment").count())
    ], ["table_name", "row_count"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Business Cases
# MAGIC In this stage, the Gold star schema is used to answer the required business questions through aggregations and joins.
# MAGIC
# MAGIC Analyses 
# MAGIC 1. Ride time analysis
# MAGIC - Average and total trip duration by date dimensions (day/month/year, day of week).
# MAGIC - Trip duration by station context (start station and end station).
# MAGIC - Trip duration by rider context (age at trip and member/casual status).
# MAGIC
# MAGIC 2. Payment analysis
# MAGIC - Total and average payment amount by month, quarter, and year.
# MAGIC - Payment totals per rider, including age at account start.
# MAGIC
# MAGIC 3. (Optional extra credit)
# MAGIC
# MAGIC - Monthly spend per member analyzed alongside:
# MAGIC   - average rides per month
# MAGIC   - total minutes ridden per month.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Analyse time spent per ride
## By day of week and month

fact_trip = spark.table("gold.fact_trip")
dim_date  = spark.table("gold.dim_date")

q1a = (
    fact_trip.alias("ft")
    .join(dim_date.alias("d"), F.col("ft.start_date_key") == F.col("d.date_key"), "left")
    .groupBy("d.year", "d.month", "d.day_of_week")
    .agg(
        F.count("*").alias("ride_count"),
        F.round(F.avg("ft.trip_duration_minutes"), 2).alias("avg_trip_minutes"),
        F.round(F.sum("ft.trip_duration_minutes"), 2).alias("total_trip_minutes")
    )
    .orderBy("d.year", "d.month", "d.day_of_week")
)

display(q1a)

## By time of day (hour)
silver_trips = spark.table("silver.trips")

q1b = (
    silver_trips
    .withColumn("hour_of_day", F.hour("started_at"))
    .groupBy("hour_of_day")
    .agg(
        F.count("*").alias("ride_count"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_trip_minutes")
    )
    .orderBy("hour_of_day")
)

display(q1b)

## By start and end station
dim_station = spark.table("gold.dim_station")

q1c = (
    fact_trip.alias("ft")
    .join(dim_station.alias("ss"), F.col("ft.start_station_id") == F.col("ss.station_id"), "left")
    .join(dim_station.alias("es"), F.col("ft.end_station_id") == F.col("es.station_id"), "left")
    .groupBy(
        F.col("ss.name").alias("start_station"),
        F.col("es.name").alias("end_station")
    )
    .agg(
        F.count("*").alias("ride_count"),
        F.round(F.avg("ft.trip_duration_minutes"), 2).alias("avg_trip_minutes")
    )
    .orderBy(F.desc("ride_count"))
)

display(q1c)

## By rider age at trip + member/casual
dim_rider = spark.table("gold.dim_rider")

q1d = (
    fact_trip.alias("ft")
    .join(dim_rider.alias("r"), "rider_id", "left")
    .withColumn(
        "member_type",
        F.when(F.col("r.is_member") == True, F.lit("member")).otherwise(F.lit("casual"))
    )
    .groupBy("member_type", "ft.rider_age_at_trip")
    .agg(
        F.count("*").alias("ride_count"),
        F.round(F.avg("ft.trip_duration_minutes"), 2).alias("avg_trip_minutes")
    )
    .orderBy("member_type", "ft.rider_age_at_trip")
)

display(q1d)


# COMMAND ----------

# 2. Analyze how much money is spent
## Per month / quarter / year

fact_payment = spark.table("gold.fact_payment")

q2a = (
    fact_payment.alias("fp")
    .join(dim_date.alias("d"), F.col("fp.payment_date_key") == F.col("d.date_key"), "left")
    .groupBy("d.year", "d.quarter", "d.month")
    .agg(
        F.count("*").alias("payment_count"),
        F.round(F.sum("fp.payment_amount"), 2).alias("total_amount"),
        F.round(F.avg("fp.payment_amount"), 2).alias("avg_payment_amount")
    )
    .orderBy("d.year", "d.quarter", "d.month")
)

display(q2a)

## Per member, based on age at account start
q2b = (
    fact_payment.alias("fp")
    .join(dim_rider.alias("r"), "rider_id", "left")
    .withColumn(
        "age_at_account_start",
        F.floor(F.months_between(F.col("r.account_start_date"), F.col("r.birthday")) / 12)
    )
    .groupBy("fp.rider_id", "age_at_account_start")
    .agg(
        F.round(F.sum("fp.payment_amount"), 2).alias("total_amount")
    )
    .orderBy(F.desc("total_amount"))
)

display(q2b)



# COMMAND ----------

# 3. Extra credit: spend per member by monthly ride behavior
## Build monthly rides + minutes per rider
rides_monthly = (
    fact_trip.alias("ft")
    .join(dim_date.alias("d"), F.col("ft.start_date_key") == F.col("d.date_key"), "left")
    .groupBy("ft.rider_id", "d.year", "d.month")
    .agg(
        F.count("*").alias("rides_per_month"),
        F.round(F.sum("ft.trip_duration_minutes"), 2).alias("minutes_per_month")
    )
)

## Build monthly spend per rider
payments_monthly = (
    fact_payment.alias("fp")
    .join(dim_date.alias("d"), F.col("fp.payment_date_key") == F.col("d.date_key"), "left")
    .groupBy("fp.rider_id", "d.year", "d.month")
    .agg(
        F.round(F.sum("fp.payment_amount"), 2).alias("spend_per_month")
    )
)

## Combine and analyze
q3 = (
    payments_monthly.alias("p")
    .join(rides_monthly.alias("r"), ["rider_id", "year", "month"], "left")
    .fillna({"rides_per_month": 0, "minutes_per_month": 0})
    .select("rider_id", "year", "month", "spend_per_month", "rides_per_month", "minutes_per_month")
    .orderBy("year", "month", "rider_id")
)

display(q3)


# COMMAND ----------

# MAGIC %md
# MAGIC # Explore Gold tables

# COMMAND ----------

spark.sql("SHOW TABLES IN gold").show(truncate=False)

# COMMAND ----------

spark.sql("DESCRIBE TABLE gold.dim_rider").show(200, truncate=False)
spark.sql("DESCRIBE TABLE gold.dim_station").show(200, truncate=False)
spark.sql("DESCRIBE TABLE gold.dim_date").show(200, truncate=False)
spark.sql("DESCRIBE TABLE gold.fact_trip").show(200, truncate=False)
spark.sql("DESCRIBE TABLE gold.fact_payment").show(200, truncate=False)

# COMMAND ----------

spark.sql("""
SELECT 'gold.dim_rider'   AS table_name, COUNT(*) AS row_count FROM gold.dim_rider
UNION ALL
SELECT 'gold.dim_station' AS table_name, COUNT(*) AS row_count FROM gold.dim_station
UNION ALL
SELECT 'gold.dim_date'    AS table_name, COUNT(*) AS row_count FROM gold.dim_date
UNION ALL
SELECT 'gold.fact_trip'   AS table_name, COUNT(*) AS row_count FROM gold.fact_trip
UNION ALL
SELECT 'gold.fact_payment' AS table_name, COUNT(*) AS row_count FROM gold.fact_payment
""").show(truncate=False)

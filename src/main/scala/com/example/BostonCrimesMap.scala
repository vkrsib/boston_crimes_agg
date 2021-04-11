package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.nio.file.Paths


object BostonCrimesMap {
  def main(args: Array[String]) = {
    if (args.length != 3) {
      println("Usage: BostonCrimesMap " +
        "<path/to/crime.csv> " +
        "<path/to/offense_codes.csv> " +
        "<path/to/output_folder>")
      sys.exit(-1)
    }

    val spark = SparkSession.builder
      .appName("BostonCrimesMap")
      .getOrCreate()

    import spark.implicits._

    val crimes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))


    val offCodes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))

    val data = crimes.dropDuplicates()
      .join(broadcast(offCodes.dropDuplicates("CODE")), crimes("OFFENSE_CODE") === offCodes("CODE"), "left")
      .drop("OFFENSE_CODE", "OFFENSE_CODE_GROUP", "OFFENSE_DESCRIPTION", "CODE")
      .drop("Location", "DAY_OF_WEEK", "HOUR", "UCR_PART", "STREET", "OCCURRED_ON_DATE")
      .drop("SHOOTING", "REPORTING_AREA")
      .filter($"DISTRICT".isNotNull)
      .withColumnRenamed("NAME", "offense_name")
      .withColumn("year_month", concat_ws("-", $"YEAR",  $"MONTH"))
      .drop("YEAR", "MONTH")
      .withColumn("offense_name", split($"offense_name", "-")(0))
      .withColumnRenamed("offense_name", "crime_type")
      .cache()

    val aggDistrict = data.groupBy("DISTRICT").agg(count("DISTRICT").alias("crimes_total"))

    val aggCoord = data.groupBy("DISTRICT")
      .agg(mean("Lat").alias("lat"), mean("Long").alias("lng"))

    val agg_top3 = data.groupBy("DISTRICT", "crime_type")
      .agg(count("crime_type").alias("district_num"))
      .withColumn("rank", rank().over(Window.partitionBy("DISTRICT").orderBy($"district_num".desc)))
      .filter($"rank" <= 3)
      .drop("rank")
      .groupBy("DISTRICT")
      .agg(concat_ws(", ", collect_list($"crime_type")).alias("frequent_crime_types"))

    val agg_monthly_med = data.groupBy("DISTRICT", "year_month")
      .count
      .groupBy("DISTRICT")
      .agg(percentile_approx($"count", lit(0.5), lit(10000))
        .alias("crimes_monthly"))

    val finalAgg = aggDistrict.join(aggCoord, "DISTRICT")
      .join(agg_top3, "DISTRICT")
      .join(agg_monthly_med, "DISTRICT")
      .withColumnRenamed("DISTRICT", "district")

    finalAgg.show(false)

    finalAgg.write.parquet(Paths.get(args(2), "boston_district_agg.parquet").toString)

    spark.stop()

  }

}

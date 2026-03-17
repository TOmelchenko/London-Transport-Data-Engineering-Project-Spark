from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, initcap, when, sum as spark_sum, avg

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_FOLDER = PROJECT_ROOT / "data" / "raw"
OUTPUT_FOLDER = PROJECT_ROOT / "data" / "output"



def create_spark_session(app_name="LondonTransportSparkProject"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.host","127.0.0.1")
        .config("spark.driver.bindAddress","127.0.0.1")
        .getOrCreate()
    )

def load_dataframes(spark):
    stations_df = (
        spark.read
        .option("header", True)
        .csv(str(RAW_DATA_FOLDER / "stations.csv"))
    )

    lines_df = (
        spark.read
        .option("header", True)
        .csv(str(RAW_DATA_FOLDER / "lines.csv"))
    )

    boroughs_df = (
        spark.read
        .option("header", True)
        .csv(str(RAW_DATA_FOLDER / "boroughs.csv"))
    )

    zones_df = (
        spark.read
        .option("header", True)
        .csv(str(RAW_DATA_FOLDER / "zones.csv"))
    )

    journeys_df = (
        spark.read
        .option("multiline", True)
        .json(str(RAW_DATA_FOLDER / "journeys.json"))
    )

    return stations_df, lines_df, boroughs_df, zones_df, journeys_df

def inspect_dataframes(stations_df, lines_df, boroughs_df, zones_df, journeys_df):
    print("\n=== Stations Schema ===")
    stations_df.printSchema()

    print("\n=== Lines Schema ===")
    lines_df.printSchema()

    print("\n=== Boroughs Schema ===")
    boroughs_df.printSchema()

    print("\n=== Zones Schema ===")
    zones_df.printSchema()

    print("\n=== Journeys Schema ===")
    journeys_df.printSchema()


def preview_data(stations_df, lines_df, boroughs_df, zones_df, journeys_df):
    print("\n=== Stations Preview ===")
    stations_df.show(5, truncate=False)

    print("\n=== Lines Preview ===")
    lines_df.show(5, truncate=False)

    print("\n=== Boroughs Preview ===")
    boroughs_df.show(5, truncate=False)

    print("\n=== Zones Preview ===")
    zones_df.show(5, truncate=False)

    print("\n=== Journeys Preview ===")
    journeys_df.show(5, truncate=False)


def clean_stations_df(stations_df):
    cleaned_df = (
        stations_df
        .filter(col("station_id").isNotNull() & (trim(col("station_id")) != ""))
        .dropDuplicates(["station_id"])
        .withColumn("station_id", trim(col("station_id")))
        .withColumn("station_name", initcap(trim(col("station_name"))))
        .withColumn("borough_id", trim(col("borough_id")))
        .withColumn("zone_id", trim(col("zone_id")))
        .withColumn("line_id", trim(col("line_id")))
        .withColumn("station_type", initcap(trim(col("station_type"))))       
    )
    return cleaned_df

def clean_lines_df(lines_df):
    cleaned_df = (
        lines_df
        .filter(col("line_id").isNotNull() & (trim(col("line_id")) != ""))
        .dropDuplicates(["line_id"])
        .withColumn("line_id", trim(col("line_id")))
        .withColumn("line_name", initcap(trim(col("line_name"))))
        .withColumn("transport_mode", initcap(trim(col("transport_mode"))))
        .withColumn("operator_id", trim(col("operator_id")))
        .withColumn("vehicle_type_id", trim(col("vehicle_type_id")))
    )
    return cleaned_df

def clean_boroughs_df(boroughs_df):
    cleaned_df = (
        boroughs_df
        .filter(col("borough_id").isNotNull() & (trim(col("borough_id")) != ""))
        .dropDuplicates(["borough_id"])
        .withColumn("borough_id", trim(col("borough_id")))
        .withColumn("borough_name", initcap(trim(col("borough_name"))))
        .withColumn("region_group", initcap(trim(col("region_group"))))
    )
    return cleaned_df

def clean_zones_df(zones_df):
    cleaned_df = (
        zones_df
        .filter(col("zone_id").isNotNull() & (trim(col("zone_id")) != ""))
        .dropDuplicates(["zone_id"])
        .withColumn("zone_id", trim(col("zone_id")))
        .withColumn("zone_name", initcap(trim(col("zone_name"))))
        .withColumn("fare_group", initcap(trim(col("fare_group"))))
    )
    return cleaned_df

def clean_journeys_df(journeys_df):
    cleaned_df = (
        journeys_df
        .filter(col("journey_id").isNotNull() & (trim(col("journey_id")) != ""))
        .filter(col("station_id").isNotNull() & (trim(col("station_id")) != ""))
        .filter(col("line_id").isNotNull() & (trim(col("line_id")) != ""))
        .filter(trim(col("passenger_count")).rlike("^[0-9]+$"))
        .filter(trim(col("delay_minutes")).rlike("^[0-9]+$"))
        .filter(trim(col("journey_date")).rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"))
        .withColumn("journey_id", trim(col("journey_id")))
        .withColumn("station_id", trim(col("station_id")))
        .withColumn("line_id", trim(col("line_id")))
        .withColumn("passenger_count", trim(col("passenger_count")).cast("int"))
        .withColumn("delay_minutes", trim(col("delay_minutes")).cast("int"))
        .withColumn("journey_date", trim(col("journey_date")).cast("date"))
        .withColumn("time_band", initcap(trim(col("time_band"))))
        .withColumn("entry_exit_flag", initcap(trim(col("entry_exit_flag"))))
    )
    return cleaned_df

def build_transport_report_df(stations_df, lines_df, boroughs_df, zones_df, journeys_df):
    report_df = (
        journeys_df.alias("j")
        .join(stations_df.alias("s"), col("j.station_id") == col("s.station_id"), "inner")
        .join(lines_df.alias("l"), col("j.line_id") == col("l.line_id"), "inner")
        .join(boroughs_df.alias("b"), col("s.borough_id") == col("b.borough_id"), "left")
        .join(zones_df.alias("z"), col("s.zone_id") == col("z.zone_id"), "left")
        .select(
            col("j.journey_id"),
            col("j.journey_date"),
            col("s.station_id"),
            col("s.station_name"),
            col("s.borough_id"),
            col("b.borough_name"),
            col("s.zone_id"),
            col("z.zone_name"),
            col("l.line_id"),
            col("l.line_name"),
            col("l.transport_mode"),
            col("j.passenger_count"),
            col("j.delay_minutes"),
            col("j.time_band"),
            col("j.entry_exit_flag")
        )
    )
    return report_df

def build_top_stations_df(report_df):
    return (
        report_df
        .groupBy("station_name")
        .agg(spark_sum("passenger_count").alias("total_passengers"))
        .orderBy(col("total_passengers").desc())
    )

def build_line_delay_df(report_df):
    return (
        report_df
        .groupBy("line_name")
        .agg(avg("delay_minutes").alias("avg_delay_minutes"))
        .orderBy(col("avg_delay_minutes").desc())
    )

def build_borough_passengers_df(report_df):
    return (
        report_df
        .groupBy("borough_name")
        .agg(spark_sum("passenger_count").alias("total_passengers"))
        .orderBy(col("total_passengers").desc())
    )

def write_outputs(report_df, top_stations_df, line_delay_df, borough_passengers_df):
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    report_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT_FOLDER / "transport_report"))
    top_stations_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT_FOLDER / "top_stations"))
    line_delay_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT_FOLDER / "line_delay"))
    borough_passengers_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(OUTPUT_FOLDER / "borough_passengers"))

def main():
    spark = create_spark_session()

    stations_df, lines_df, boroughs_df, zones_df, journeys_df = load_dataframes(spark)

    inspect_dataframes(stations_df, lines_df, boroughs_df, zones_df, journeys_df)
    preview_data(stations_df, lines_df, boroughs_df, zones_df, journeys_df)

    stations_clean_df = clean_stations_df(stations_df)
    lines_clean_df = clean_lines_df(lines_df)
    boroughs_clean_df = clean_boroughs_df(boroughs_df)
    zones_clean_df = clean_zones_df(zones_df)
    journeys_clean_df = clean_journeys_df(journeys_df)

    report_df = build_transport_report_df(
        stations_clean_df,
        lines_clean_df,
        boroughs_clean_df,
        zones_clean_df,
        journeys_clean_df
    )

    top_stations_df = build_top_stations_df(report_df)
    line_delay_df = build_line_delay_df(report_df)
    borough_passengers_df = build_borough_passengers_df(report_df)

    print("\n=== Final Transport Report Preview ===")
    report_df.show(10, truncate=False)

    print("\n=== Top Stations Preview ===")
    top_stations_df.show(10, truncate=False)

    print("\n=== Line Delay Preview ===")
    line_delay_df.show(10, truncate=False)

    print("\n=== Borough Passengers Preview ===")
    borough_passengers_df.show(10, truncate=False)

    write_outputs(report_df, top_stations_df, line_delay_df, borough_passengers_df)

    spark.stop()

    print("\nSpark pipeline completed successfully.")

if __name__ == "__main__":
    main()      
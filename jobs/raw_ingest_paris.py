import argparse
import datetime
import requests
from urllib.parse import quote
from pyspark.sql import SparkSession, functions as F, types as T

# >>> Vecteur des villes (20)
CITIES = [
    "Paris", "Marseille", "Lyon", "Toulouse", "Nice",
    "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille",
    "Rennes", "Reims", "Toulon", "Grenoble", "Dijon",
    "Angers", "Nimes", "Clermont-Ferrand", "Le Havre", "Saint-Etienne",
]

import os
WAQI_TOKEN = os.environ.get("WAQI_TOKEN")
OW_TOKEN = os.environ.get("OW_TOKEN")

RAW_WAQI = "hdfs://hadoop:9000/data/raw/waqi"
RAW_OW   = "hdfs://hadoop:9000/data/raw/openweather"


def fetch(url: str):
    try:
        r = requests.get(url, timeout=30)
        return r.status_code, r.text
    except Exception as e:
        return 0, f"ERROR: {e}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-ts", dest="run_ts", required=True)
    args = parser.parse_args()

    # run_ts est déjà Europe/Paris (fourni par le DAG)
    run_dt = datetime.datetime.fromisoformat(args.run_ts)
    fetch_dt = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    spark = (
        SparkSession.builder
        .appName("raw_ingest_paris")
        .config("spark.sql.session.timeZone", "Europe/Paris")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    rows = []
    for city in CITIES:
        # URLs construites par nom de ville (sans rien changer d’autre)
        waqi_url = f"https://api.waqi.info/feed/{quote(city)}/?token={WAQI_TOKEN}"
        ow_url   = f"https://api.openweathermap.org/data/2.5/weather?q={quote(city+',fr')}&appid={OW_TOKEN}&units=metric&lang=fr"

        for url in (waqi_url, ow_url):
            status, payload = fetch(url)
            rows.append({
                "city_name": city,
                "url": url,
                "http_status": int(status),
                "ok": bool(status == 200),
                "fetched_at_utc": fetch_dt.isoformat().replace("+00:00", "Z"),
                "payload": payload,
            })

    schema = T.StructType([
        T.StructField("city_name", T.StringType()),   # <-- ajouté
        T.StructField("url", T.StringType()),
        T.StructField("http_status", T.IntegerType()),
        T.StructField("ok", T.BooleanType()),
        T.StructField("fetched_at_utc", T.StringType()),
        T.StructField("payload", T.StringType()),
    ])

    df = (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("fetched_at_utc", F.to_timestamp("fetched_at_utc"))
        .withColumn("dt",   F.lit(run_dt.date().isoformat()).cast("date"))
        .withColumn("hour", F.lit(run_dt.strftime("%H")))
    )

    df_w = df.filter(F.col("url").contains("waqi.info"))
    df_o = df.filter(F.col("url").contains("openweathermap.org"))

    # (on ne change PAS les partitions/écritures)
    df_w.write.mode("overwrite").partitionBy("dt", "hour").json(RAW_WAQI)
    df_o.write.mode("overwrite").partitionBy("dt", "hour").json(RAW_OW)

    spark.stop()


if __name__ == "__main__":
    main()

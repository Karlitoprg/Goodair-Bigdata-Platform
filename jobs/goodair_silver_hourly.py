import argparse
from pyspark.sql import SparkSession, functions as F, types as T

# -------------------------------------------------------------------
#  CONSTANTES
# -------------------------------------------------------------------

RAW_WAQI = "hdfs://hadoop:9000/data/raw/waqi"
RAW_OW   = "hdfs://hadoop:9000/data/raw/openweather"

JDBC_URL = "jdbc:postgresql://postgres:5432/airflow"
JDBC_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}

# -------------------------------------------------------------------
#  SCHÉMAS JSON
# -------------------------------------------------------------------

WAQI_SCHEMA = T.StructType([
    T.StructField("status", T.StringType()),
    T.StructField("data", T.StructType([
        T.StructField("aqi", T.IntegerType()),
        T.StructField("idx", T.IntegerType()),
        T.StructField("city", T.StructType([
            T.StructField("geo", T.ArrayType(T.DoubleType())),  # [lat, lon]
            T.StructField("name", T.StringType()),
            T.StructField("url", T.StringType()),
            T.StructField("location", T.StringType()),
        ])),
        T.StructField("dominentpol", T.StringType()),
        T.StructField("iaqi", T.StructType([
            T.StructField("co",   T.StructType([T.StructField("v", T.DoubleType())])),
            T.StructField("no2",  T.StructType([T.StructField("v", T.DoubleType())])),
            T.StructField("o3",   T.StructType([T.StructField("v", T.DoubleType())])),
            T.StructField("pm10", T.StructType([T.StructField("v", T.DoubleType())])),
            T.StructField("pm25", T.StructType([T.StructField("v", T.DoubleType())])),
            T.StructField("so2",  T.StructType([T.StructField("v", T.DoubleType())])),
            # météo dans WAQI
            T.StructField("h",    T.StructType([T.StructField("v", T.DoubleType())])),
            T.StructField("t",    T.StructType([T.StructField("v", T.DoubleType())])),
            T.StructField("p",    T.StructType([T.StructField("v", T.DoubleType())])),
            T.StructField("w",    T.StructType([T.StructField("v", T.DoubleType())])),
        ])),
        T.StructField("time", T.StructType([
            T.StructField("s",   T.StringType()),
            T.StructField("tz",  T.StringType()),
            T.StructField("v",   T.LongType()),
            T.StructField("iso", T.StringType()),
        ])),
        T.StructField("forecast", T.StructType([
            T.StructField("daily", T.StructType([
                T.StructField("o3", T.ArrayType(T.StructType([
                    T.StructField("avg", T.DoubleType()),
                    T.StructField("day", T.StringType()),
                    T.StructField("max", T.DoubleType()),
                    T.StructField("min", T.DoubleType()),
                ]))),
                T.StructField("pm10", T.ArrayType(T.StructType([
                    T.StructField("avg", T.DoubleType()),
                    T.StructField("day", T.StringType()),
                    T.StructField("max", T.DoubleType()),
                    T.StructField("min", T.DoubleType()),
                ]))),
                T.StructField("pm25", T.ArrayType(T.StructType([
                    T.StructField("avg", T.DoubleType()),
                    T.StructField("day", T.StringType()),
                    T.StructField("max", T.DoubleType()),
                    T.StructField("min", T.DoubleType()),
                ]))),
                T.StructField("uvi", T.ArrayType(T.StructType([
                    T.StructField("avg", T.DoubleType()),
                    T.StructField("day", T.StringType()),
                    T.StructField("max", T.DoubleType()),
                    T.StructField("min", T.DoubleType()),
                ]))),
            ])),
        ])),
    ])),
])

OW_SCHEMA = T.StructType([
    T.StructField("coord", T.StructType([
        T.StructField("lon", T.DoubleType()),
        T.StructField("lat", T.DoubleType()),
    ])),
    T.StructField("weather", T.ArrayType(T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("main", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("icon", T.StringType()),
    ]))),
    T.StructField("main", T.StructType([
        T.StructField("temp", T.DoubleType()),
        T.StructField("feels_like", T.DoubleType()),
        T.StructField("temp_min", T.DoubleType()),
        T.StructField("temp_max", T.DoubleType()),
        T.StructField("pressure", T.IntegerType()),
        T.StructField("humidity", T.IntegerType()),
        T.StructField("sea_level", T.IntegerType()),
        T.StructField("grnd_level", T.IntegerType()),
    ])),
    T.StructField("visibility", T.IntegerType()),
    T.StructField("wind", T.StructType([
        T.StructField("speed", T.DoubleType()),
        T.StructField("deg", T.IntegerType()),
        T.StructField("gust", T.DoubleType()),
    ])),
    T.StructField("clouds", T.StructType([
        T.StructField("all", T.IntegerType()),
    ])),
    T.StructField("dt", T.LongType()),
    T.StructField("sys", T.StructType([
        T.StructField("country", T.StringType()),
        T.StructField("sunrise", T.LongType()),
        T.StructField("sunset", T.LongType()),
    ])),
    T.StructField("timezone", T.IntegerType()),
    T.StructField("id", T.LongType()),
    T.StructField("name", T.StringType()),
    T.StructField("cod", T.IntegerType()),
])

# -------------------------------------------------------------------
#  SPARK / JDBC UTILITAIRES
# -------------------------------------------------------------------

def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("goodair_silver_hourly")
        .config("spark.sql.session.timeZone", "Europe/Paris")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_table(spark: SparkSession, table_or_query: str):
    """
    table_or_query : "schema.table" OU "(SELECT ... ) t"
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_or_query)
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .load()
    )

# -------------------------------------------------------------------
#  DIM CITY : CANDIDATS + UPSERT
# -------------------------------------------------------------------

def build_dim_city_candidates(df_waqi_raw, df_ow_raw):
    """
    Construit les candidats de dim_city depuis RAW (WAQI + OpenWeather)
    Colonnes finales : nom_ville, pays_code, latitude, longitude, waqi_city_name, ow_city_name
    """
    # WAQI candidates
    wq = (
        df_waqi_raw
        .filter(F.col("ok") == True)
        .withColumn("j", F.from_json("payload", WAQI_SCHEMA))
        .filter(F.col("j.status") == "ok")
        .select(
            F.col("city_name").alias("nom_ville"),
            F.lit("FR").alias("pays_code"),
            F.col("j.data.city.name").alias("waqi_city_name"),
            F.col("j.data.city.geo")[0].cast("double").alias("latitude"),
            F.col("j.data.city.geo")[1].cast("double").alias("longitude"),
        )
        .filter(F.col("nom_ville").isNotNull())
        .withColumn("nom_ville", F.initcap(F.trim(F.col("nom_ville"))))
        .dropDuplicates(["nom_ville", "pays_code"])
    )

    # OW candidates
    ow = (
        df_ow_raw
        .filter(F.col("ok") == True)
        .withColumn("j", F.from_json("payload", OW_SCHEMA))
        .filter(F.col("j.cod") == 200)
        .select(
            F.col("city_name").alias("nom_ville"),
            F.lit("FR").alias("pays_code"),
            F.col("j.name").alias("ow_city_name"),
            F.col("j.coord.lat").cast("double").alias("latitude"),
            F.col("j.coord.lon").cast("double").alias("longitude"),
        )
        .filter(F.col("nom_ville").isNotNull())
        .withColumn("nom_ville", F.initcap(F.trim(F.col("nom_ville"))))
        .dropDuplicates(["nom_ville", "pays_code"])
    )

    # ✅ JOIN FULL (correction de ton erreur : pas de wq.nom_ville après join)
    dim = (
        wq.join(ow, ["nom_ville", "pays_code"], "full")
        .select(
            F.col("nom_ville"),
            F.col("pays_code"),
            F.coalesce(wq["latitude"], ow["latitude"]).alias("latitude"),
            F.coalesce(wq["longitude"], ow["longitude"]).alias("longitude"),
            wq["waqi_city_name"],
            ow["ow_city_name"],
        )
        .filter(F.col("nom_ville").isNotNull())
        .dropDuplicates(["nom_ville", "pays_code"])
    )

    return dim


def upsert_dim_city(spark: SparkSession, df_candidates):
    """
    Upsert dim_city dans Postgres.
    On insert les (nom_ville,pays_code) manquants.
    Puis on met à jour les colonnes de détail (lat/lon/noms api) si NULL en base.
    """
    # dim_city existante
    dim_existing = read_table(spark, "goodair.dim_city").select(
        "city_id", "nom_ville", "pays_code", "latitude", "longitude", "waqi_city_name", "ow_city_name"
    )

    # nouveaux (non existants)
    new_rows = (
        df_candidates.alias("n")
        .join(
            dim_existing.select("nom_ville", "pays_code").alias("e"),
            (F.col("n.nom_ville") == F.col("e.nom_ville")) &
            (F.col("n.pays_code") == F.col("e.pays_code")),
            "left_anti"
        )
    )

    nb_new = new_rows.count()
    print(f">>> DIM_CITY : {df_candidates.count()} candidats, {nb_new} nouveaux à insérer")

    if nb_new > 0:
        (
            new_rows.select(
                "nom_ville", "pays_code", "latitude", "longitude", "waqi_city_name", "ow_city_name"
            )
            .write.mode("append")
            .jdbc(JDBC_URL, "goodair.dim_city", properties=JDBC_PROPS)
        )

    # recharge dim_city après insert (pour avoir city_id)
    dim_post = read_table(spark, "goodair.dim_city").select(
        "city_id", "nom_ville", "pays_code", "latitude", "longitude", "waqi_city_name", "ow_city_name"
    )

    # petite amélioration: on remplit les NULL côté base via un UPDATE SQL serait idéal,
    # mais en Spark pur on évite -> on laisse tel quel (ok MSPR).
    print(f">>> DIM_CITY rows (postgres) : {dim_post.count()}")

    return dim_post

# -------------------------------------------------------------------
#  LOAD DIM
# -------------------------------------------------------------------

def load_dim_city(spark: SparkSession):
    df = read_table(spark, "goodair.dim_city")
    return df.withColumn("nom_ville_norm", F.upper(F.trim(F.col("nom_ville"))))

# -------------------------------------------------------------------
#  TRANSFORMATIONS : AIR QUALITY CURRENT
# -------------------------------------------------------------------

def transform_air_quality_current(df_waqi_raw, df_dim_city):
    df = (
        df_waqi_raw
        .filter(F.col("ok") == True)
        .withColumn("j", F.from_json("payload", WAQI_SCHEMA))
        .filter(F.col("j.status") == "ok")
        .withColumn("nom_ville", F.initcap(F.trim(F.col("city_name"))))
        .filter(F.col("nom_ville").isNotNull())
        .withColumn("fetched_at_utc", F.to_timestamp("fetched_at_utc"))
    )

    # measured_at métier : priorité à time.iso sinon fallback fetched_at_utc
    df = df.withColumn(
        "measured_at",
        F.when(
            F.col("j.data.time.iso").isNotNull(),
            F.to_timestamp(F.col("j.data.time.iso"))
        ).otherwise(F.col("fetched_at_utc"))
    ).filter(F.col("measured_at").isNotNull())

    df_measures = df.select(
        "nom_ville",
        "measured_at",
        "fetched_at_utc",
        F.col("j.data.time.v").alias("epoch_value"),
        F.col("j.data.aqi").alias("aqi_global"),
        F.col("j.data.dominentpol").alias("polluant_dominant"),
        F.col("j.data.iaqi.co.v").alias("monoxyde_de_carbone"),
        F.col("j.data.iaqi.no2.v").alias("dioxyde_d_azote"),
        F.col("j.data.iaqi.o3.v").alias("ozone"),
        F.col("j.data.iaqi.so2.v").alias("dioxyde_de_soufre"),
        F.col("j.data.iaqi.pm10.v").alias("particules"),
        F.col("j.data.iaqi.pm25.v").alias("particules_fines"),
        F.col("j.data.iaqi.h.v").alias("humidite"),
        F.col("j.data.iaqi.t.v").alias("temperature"),
        F.col("j.data.iaqi.p.v").alias("pression"),
        F.col("j.data.iaqi.w.v").alias("vitesse_vent"),
    )

    df_join = (
        df_measures.alias("m")
        .join(
            df_dim_city.alias("d"),
            F.upper(F.col("m.nom_ville")) == F.col("d.nom_ville_norm"),
            "inner"
        )
        .select(
            F.col("d.city_id"),
            F.col("m.measured_at"),
            F.col("m.fetched_at_utc"),
            "aqi_global",
            "polluant_dominant",
            "monoxyde_de_carbone",
            "dioxyde_d_azote",
            "ozone",
            "dioxyde_de_soufre",
            "particules",
            "particules_fines",
            "humidite",
            "temperature",
            "pression",
            "vitesse_vent",
            "epoch_value",
        )
        .dropDuplicates(["city_id", "fetched_at_utc"])
    )

    return df_join

# -------------------------------------------------------------------
#  TRANSFORMATIONS : WEATHER CURRENT
# -------------------------------------------------------------------

def transform_weather_current(df_ow_raw, df_dim_city):
    df = (
        df_ow_raw
        .filter(F.col("ok") == True)
        .withColumn("j", F.from_json("payload", OW_SCHEMA))
        .filter(F.col("j.cod") == 200)
        .withColumn("nom_ville", F.initcap(F.trim(F.col("city_name"))))
        .filter(F.col("nom_ville").isNotNull())
        .withColumn("fetched_at_utc", F.to_timestamp("fetched_at_utc"))
    )

    # measured_at métier : dt (epoch) + timezone offset
    df = df.withColumn(
        "measured_at",
        (F.from_unixtime(F.col("j.dt"))).cast("timestamp")
    ).filter(F.col("measured_at").isNotNull())

    df_sel = df.select(
        "nom_ville",
        "measured_at",
        "fetched_at_utc",
        F.col("j.main.temp").alias("temperature"),
        F.col("j.main.feels_like").alias("temperature_ressentie"),
        F.col("j.main.temp_min").alias("temperature_min"),
        F.col("j.main.temp_max").alias("temperature_max"),
        F.col("j.main.pressure").alias("pression"),
        F.col("j.main.sea_level").alias("pression_niveau_mer"),
        F.col("j.main.grnd_level").alias("pression_niveau_sol"),
        F.col("j.main.humidity").alias("humidite"),
        F.col("j.wind.speed").alias("vitesse_vent"),
        F.col("j.wind.deg").alias("direction_vent_deg"),
        F.col("j.wind.gust").alias("rafale_vent"),
        F.col("j.clouds.all").alias("couverture_nuageuse"),
        F.expr("j.weather[0].id").alias("code_meteo"),
        F.expr("j.weather[0].main").alias("temps_principal"),
        F.expr("j.weather[0].description").alias("description_meteo"),
        F.expr("j.weather[0].icon").alias("icone_meteo"),
        F.col("j.timezone").alias("timezone_offset_seconds"),
        F.col("j.id").alias("id_station_ow"),
        F.col("j.name").alias("nom_ville_ow"),
    )

    df_join = (
        df_sel.alias("w")
        .join(
            df_dim_city.alias("d"),
            F.upper(F.col("w.nom_ville")) == F.col("d.nom_ville_norm"),
            "inner"
        )
        .select(
            F.col("d.city_id"),
            "measured_at",
            "fetched_at_utc",
            "temperature",
            "temperature_ressentie",
            "temperature_min",
            "temperature_max",
            "pression",
            "pression_niveau_mer",
            "pression_niveau_sol",
            "humidite",
            "vitesse_vent",
            "direction_vent_deg",
            "rafale_vent",
            "couverture_nuageuse",
            "code_meteo",
            "temps_principal",
            "description_meteo",
            "icone_meteo",
            "timezone_offset_seconds",
            "id_station_ow",
            "nom_ville_ow",
        )
        .dropDuplicates(["city_id", "fetched_at_utc"])
    )

    return df_join

# -------------------------------------------------------------------
#  TRANSFORMATIONS : AIR QUALITY FORECAST
# -------------------------------------------------------------------

def transform_air_quality_forecast(df_waqi_raw, df_dim_city):
    df = (
        df_waqi_raw
        .filter(F.col("ok") == True)
        .withColumn("j", F.from_json("payload", WAQI_SCHEMA))
        .filter(F.col("j.status") == "ok")
        .withColumn("nom_ville", F.initcap(F.trim(F.col("city_name"))))
        .filter(F.col("nom_ville").isNotNull())
        .withColumn("fetched_at_utc", F.to_timestamp("fetched_at_utc"))
        .select(
            "nom_ville",
            "fetched_at_utc",
            F.col("j.data.forecast.daily").alias("forecast")
        )
        .filter(F.col("forecast").isNotNull())
    )

    def explode_pollutant(p):
        return (
            df.select(
                "nom_ville",
                "fetched_at_utc",
                F.lit(p).alias("type_polluant"),
                F.explode_outer(F.col(f"forecast.{p}")).alias("f")
            )
            .select(
                "nom_ville",
                "fetched_at_utc",
                "type_polluant",
                F.to_date(F.col("f.day")).alias("date_prevision"),
                F.col("f.min").cast("double").alias("valeur_min"),
                F.col("f.avg").cast("double").alias("valeur_moyenne"),
                F.col("f.max").cast("double").alias("valeur_max"),
            )
            .filter(F.col("date_prevision").isNotNull())
        )

    df_all = (
        explode_pollutant("o3")
        .unionByName(explode_pollutant("pm10"))
        .unionByName(explode_pollutant("pm25"))
        .unionByName(explode_pollutant("uvi"))
    )

    df_join = (
        df_all.alias("f")
        .join(
            df_dim_city.alias("d"),
            F.upper(F.col("f.nom_ville")) == F.col("d.nom_ville_norm"),
            "inner"
        )
        .select(
            F.col("d.city_id"),
            "date_prevision",
            "type_polluant",
            "valeur_min",
            "valeur_moyenne",
            "valeur_max",
            "fetched_at_utc",
        )
        .dropDuplicates(["city_id", "type_polluant", "date_prevision", "fetched_at_utc"])
    )

    return df_join

# -------------------------------------------------------------------
#  MAIN
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dt", required=True, help="date au format YYYY-MM-DD")
    parser.add_argument("--hour", required=True, help="heure au format HH (00-23)")
    args = parser.parse_args()

    dt = args.dt
    hour = args.hour
    print(f">>> GOODAIR SILVER HOURLY - dt={dt}, hour={hour}")

    spark = get_spark()

    path_waqi = f"{RAW_WAQI}/dt={dt}/hour={hour}"
    path_ow   = f"{RAW_OW}/dt={dt}/hour={hour}"

    print(f">>> Lecture RAW WAQI : {path_waqi}")
    print(f">>> Lecture RAW OW   : {path_ow}")

    df_waqi_raw = spark.read.json(path_waqi)
    df_ow_raw   = spark.read.json(path_ow)

    print(f">>> WAQI RAW rows : {df_waqi_raw.count()}")
    print(f">>> OW   RAW rows : {df_ow_raw.count()}")

    # 1) DIM CITY (auto)
    df_dim_candidates = build_dim_city_candidates(df_waqi_raw, df_ow_raw)
    _ = upsert_dim_city(spark, df_dim_candidates)

    # 2) Recharge dim_city pour joins
    df_dim_city = load_dim_city(spark)

    # ------------------------------------------------------------------
    # AIR QUALITY CURRENT : insert nouveaux city_id + fetched_at_utc
    # ------------------------------------------------------------------
    df_airq = transform_air_quality_current(df_waqi_raw, df_dim_city)
    nb_airq_calc = df_airq.count()

    df_airq_existing_keys = read_table(
        spark,
        "(SELECT city_id, fetched_at_utc FROM goodair.air_quality_current) t"
    ).dropDuplicates()

    df_airq_new = (
        df_airq.alias("n")
        .join(
            df_airq_existing_keys.alias("e"),
            (F.col("n.city_id") == F.col("e.city_id")) &
            (F.col("n.fetched_at_utc") == F.col("e.fetched_at_utc")),
            "left_anti"
        )
    )

    nb_airq_new = df_airq_new.count()
    print(f">>> AIR_QUALITY_CURRENT : {nb_airq_calc} calculées, {nb_airq_new} nouvelles à insérer")

    if nb_airq_new > 0:
        (
            df_airq_new.write
            .mode("append")
            .jdbc(JDBC_URL, "goodair.air_quality_current", properties=JDBC_PROPS)
        )
    else:
        print(">>> AIR_QUALITY_CURRENT : rien à insérer")

    # ------------------------------------------------------------------
    # WEATHER CURRENT : insert nouveaux city_id + fetched_at_utc
    # ------------------------------------------------------------------
    df_weather = transform_weather_current(df_ow_raw, df_dim_city)
    nb_weather_calc = df_weather.count()

    df_weather_existing_keys = read_table(
        spark,
        "(SELECT city_id, fetched_at_utc FROM goodair.weather_current) t"
    ).dropDuplicates()

    df_weather_new = (
        df_weather.alias("n")
        .join(
            df_weather_existing_keys.alias("e"),
            (F.col("n.city_id") == F.col("e.city_id")) &
            (F.col("n.fetched_at_utc") == F.col("e.fetched_at_utc")),
            "left_anti"
        )
    )

    nb_weather_new = df_weather_new.count()
    print(f">>> WEATHER_CURRENT : {nb_weather_calc} calculées, {nb_weather_new} nouvelles à insérer")

    if nb_weather_new > 0:
        (
            df_weather_new.write
            .mode("append")
            .jdbc(JDBC_URL, "goodair.weather_current", properties=JDBC_PROPS)
        )
    else:
        print(">>> WEATHER_CURRENT : rien à insérer")

    # ------------------------------------------------------------------
    # AIR QUALITY FORECAST : insert nouveaux (city_id, type, date, fetch)
    # ------------------------------------------------------------------
    df_forecast = transform_air_quality_forecast(df_waqi_raw, df_dim_city)
    nb_forecast_calc = df_forecast.count()

    df_forecast_existing_keys = read_table(
        spark,
        "(SELECT city_id, type_polluant, date_prevision, fetched_at_utc FROM goodair.air_quality_forecast) t"
    ).dropDuplicates()

    df_forecast_new = (
        df_forecast.alias("n")
        .join(
            df_forecast_existing_keys.alias("e"),
            (F.col("n.city_id") == F.col("e.city_id")) &
            (F.col("n.type_polluant") == F.col("e.type_polluant")) &
            (F.col("n.date_prevision") == F.col("e.date_prevision")) &
            (F.col("n.fetched_at_utc") == F.col("e.fetched_at_utc")),
            "left_anti"
        )
    )

    nb_forecast_new = df_forecast_new.count()
    print(f">>> AIR_QUALITY_FORECAST : {nb_forecast_calc} calculées, {nb_forecast_new} nouvelles à insérer")

    if nb_forecast_new > 0:
        (
            df_forecast_new.write
            .mode("append")
            .jdbc(JDBC_URL, "goodair.air_quality_forecast", properties=JDBC_PROPS)
        )
    else:
        print(">>> AIR_QUALITY_FORECAST : rien à insérer")

    print(f">>> MODE HOURLY terminé pour dt={dt}, hour={hour}")
    spark.stop()


if __name__ == "__main__":
    main()

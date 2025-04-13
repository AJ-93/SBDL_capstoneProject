import configparser

from pyspark import SparkConf
from pyspark.sql import SparkSession

KAFKA_JAR = "jars/spark-sql-kafka-0-10_2.12-3.4.4.jar"

def get_spark_session(env):
    if env == "LOCAL":
        spark_conf = SparkConf()
        config = configparser.ConfigParser()
        config.read("conf/spark.conf")

        items = list(config.items("LOCAL"))[:2]
        for key, value in items:
            spark_conf.set(key, value)

        return SparkSession.builder \
                .config("spark.driver.extraJavaOptions",
                        "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=sbdl-logs") \
                .config("spark.jars.packages", KAFKA_JAR) \
                .config(conf=spark_conf) \
                .enableHiveSupport() \
                .getOrCreate()

def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}
    for key, value in config.items(env):
        conf[key] = value
    return conf

def get_data_filter(env, data_filter):
    conf = get_app_config(env)
    return "true" if conf.get(data_filter) == ""  else conf.get(data_filter)

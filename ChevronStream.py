from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark.conf.set("spark.sql.shuffle.partitions", 8)

spark = SparkSession \
    .builder \
    .appName("SocketParse") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to localhost:9999
# Run Netcat and consume input $nc -lk 9999
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()

# This is the regular expression pattern that we will use later.
IP_REG_EX = """^.*\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*$"""

# Start running the query that prints the running counts to the console
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

# Imports for file import.
#import requests
#from urllib import urlopen
from pandas import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from pyspark.sql.functions import explode

BabyFile = "/Users/justinmichaels/babynames.json"
BabyDataFile = "/Users/justinmichaels/babyDataFile.txt"

# Manual translation of the babynames dataset.
theSchema = StructType([
    StructField("sid", StringType(), True)
    , StructField("id", StringType(), True)
    , StructField("position", StringType(), True)
    , StructField("created_at", TimestampType(), True)
    , StructField("created_meta", StringType(), True)
    , StructField("updated_at", TimestampType(), True)
    , StructField("updated_meta", StringType(), True)
    , StructField("meta", StringType(), True)
    , StructField("Year", IntegerType(), True)
    , StructField("FirstName", StringType(), True)
    , StructField("Country", StringType(), True)
    , StructField("Count", LongType(), True)
    , StructField("Sex", StringType(), True)
])

# Get a file from somewhere to use. For large files writing chunks will improve reliability.
# Once we get the file we don't need to do this every time.
# urlString = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json?accessType=DOWNLOAD"
# response = urlopen(urlString)
# CHUNK = 16 * 1024
# with open(BabyFile, 'wb') as f:
#     while True:
#       chunk = response.read(CHUNK)
#       if not chunk:
#           break
#     f.write(chunk)

# Establish our Spark entry point for data analysis and transformation.
spark = SparkSession \
    .builder \
    .config("spark.debug.maxToStringFields", 100) \
    .appName("BabyNames") \
    .getOrCreate()

# Establish a dataframe based on the JSON input file.
# Infering schema will have performance overhead; however, useful for unknown data structures.
babyDF = spark.read\
    .option("multiline", "true") \
    .option("inferSchema", "true") \
    .json(BabyFile)

# Validate the schema is accurate as a sanity check and a way to understand inbound data structure.
#babyDF.printSchema()

# JSON parsing
# The schems is documented in the JSON meta attribute.
temp = babyDF.select("meta.view.columns.name")

# Some debug checking
temp.printSchema()
# temp.show()

# Get the schema from the dataframe.
columnNames = temp.select(
    explode(col="name").alias("columns")
).toPandas() #.rdd.flatMap(lambda x: x) #.collect()

print(columnNames)

# The data itself is an attribute of the file called data.
babyData = babyDF.select(
    explode("data")
).repartition(4) #.write.mode("overwrite").json(BabyDataFile)  #.schema(theSchema)
# We could output information for durability.
# Or read additonal information into the data pipeline.

# Use pic from here ... http://www.cs.sfu.ca/~ggbaker/data-science/content/bigdata.html ... to illustrate
# that there won't be a single file created but directories of files.
# rawData = spark.read.json(BabyDataFile).select("data")

# The information in the data frame is stored in an array.
# Here we can reach into the arrays to get details.
# This can be chained on the dataframe above but illustrates the pipeline.
babyData2 = babyData.select(babyData["col"].getItem(0)
                , babyData["col"].getItem(1)
                , babyData["col"].getItem(2)
                , babyData["col"].getItem(3)
                , babyData["col"].getItem(4)
                , babyData["col"].getItem(5)
                , babyData["col"].getItem(6)
                , babyData["col"].getItem(7)
                , babyData["col"].getItem(8)
                , babyData["col"].getItem(9)
                , babyData["col"].getItem(10)
                , babyData["col"].getItem(11)
                , babyData["col"].getItem(12)
                ).coalesce(6) #.schema(theSchema)

# TODO We could alias the fields above but we should be able to progamatically add the schema using "columnNames" from above.

# Some debug checking
babyData2.printSchema()
babyData2.show(10)
partitions = babyData2.rdd.getNumPartitions()
print(partitions)
import json
# import pandas as pd
from pyspark.sql import SparkSession
from pyspark.shell import spark
from setuptools._vendor.pyparsing import col

spark = SparkSession \
    .builder \
    .config("spark.debug.maxToStringFields", 256) \
    .appName("TweetsExample") \
    .getOrCreate()

tweets_data_path = "/Users/justinmichaels/twitterdata.txt"
tweets_delta_path = "/Users/justinmichaels/"

tweets_data = []
tweets_file = open(tweets_data_path, "r")
for line in tweets_file:
    try:
        tweet = json.loads(line)
        tweets_data.append(tweet)
    except:
        continue

# Using pandas dataframe could be used.
# tweetP = pd.DataFrame()
# tweetP['text'] = map(lambda tweet: tweet['text'], tweets_data)
# tweetP['lang'] = map(lambda tweet: tweet['lang'], tweets_data)
# tweetP['country'] = map(lambda tweet: tweet['place']['country'] if tweet['place'] != None else None, tweets_data)

# Consume tweets as a dataframe.
tweetDF = spark.read \
     .option("inferSchema", "true") \
     .json(tweets_data_path)
tweetDF.printSchema()
# Given the rich data structure we can pull only the appropriate fields required.
#tweetDF.createOrReplaceTempView("thetweets")
tweetDFfilter = tweetDF.select("user.name", "lang", "place", "text").where( "lang == 'en'")
tweetDFfilter.show(20, False)

# Delta Table will run in Databricks
# tweetDF.write\
#     .format("delta")\
#     .mode("append")\
#     .save("/Users/justinmichaels/twitterDelta")

#TODO Structured Streaming (Tumbling vs Sliding Windows)


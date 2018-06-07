#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
#Import the Spark entry point
from pyspark.sql import SparkSession

#Variables that contains the user credentials to access Twitter API
access_token = "add your token"
access_token_secret = "add your token secret"
consumer_key = "add your key"
consumer_secret = "add your secret"

spark = SparkSession \
    .builder \
    .config("spark.debug.maxToStringFields", 256) \
    .appName("RawTweets") \
    .getOrCreate()

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        file = open("/Users/justinmichaels/twitterdata.txt","a")
        file.write(data)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['databricks', 'Databricks', '#databricks', '#Databricks', 'Azure', '#Azure'])


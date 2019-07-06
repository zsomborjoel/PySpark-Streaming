#current date (yyyy.mm.dd.)
import time
date = time.strftime("%Y%m%d")
currtime = time.strftime("%X")

from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import happybase
import json

#Hbase connection
connection = happybase.Connection('localhost')
connection.open()

#Spark base
sc = SparkContext()
spark = SparkSession(sc)
ssc = StreamingContext(sc,1)

#Kafka stream
kvs = KafkaUtils.createDirectStream(ssc, ['twitter_data'], {"metadata.broker.list":"localhost:9092"})

#parse incoming json from stream to json again
parsed = kvs.map(lambda v: json.loads(v[1]))


def month_string_to_number(string):
    m = {
        'jan': '01',
        'feb': '02',
        'mar': '03',
        'apr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'aug': '08',
        'sep': '09',
        'oct': '10',
        'nov': '11',
        'dec': '12'
        }
    s = string.strip().lower()
    out = m[s]
    return out

def log_error(e):
    error_log = open("tw_stream_data_hbase_consumer_error_log" + date + ".txt", "a+")
    error_log.write(str(currtime) + ' - Streaming error happened: ' + str(e) + '\n')
    error_log.close()

#write data into Cassandra
def write_to_db(rdd):
    for i in rdd.collect():
        try:
            tweet_id = i['id']
            user_id = i['user']['id']
            user_name = i['user']['name']
            user_screen_name = i['user']['screen_name']
            followers_count = i['user']['followers_count']
            friends_count = i['user']['friends_count']
            statuses_count = i['user']["statuses_count"]

            pre_acd = i['user']['created_at']
            account_create_datetime = (
                                        pre_acd[26:30] +
                                        '-' +
                                        month_string_to_number(pre_acd[4:7]) +
                                        '-' +
                                        pre_acd[8:10] +
                                        ' ' +
                                        pre_acd[11:19]
                                        )

            pre_tcd = i['created_at']
            tweet_create_datetime = (
                                    pre_tcd[26:30] +
                                    '-' +
                                    month_string_to_number(pre_tcd[4:7]) +
                                    '-' +
                                    pre_tcd[8:10] +
                                    ' ' +
                                    pre_tcd[11:19]
                                    )

            tweet_text = i['text'].replace("'", "")
            tweet_topic = 'data'
            retweeted_flag = i['retweeted']
            sensitive_tweet_flag = i['possibly_sensitive']
        except:
            retweeted_flag = False
            sensitive_tweet_flag = False

        #composite key
        row_id = str(user_id) + '-' + str(tweet_id)

        tweets = connection.table('tweets')
        users = connection.table('users')

        try:
            #tweets insert
            tweets.put(row_id,
                       {'tweet_main_info:user_name': str(user_name),
                        'tweet_main_info:tweet_create_datetime': str(tweet_create_datetime),
                        'tweet_main_info:tweet_text': str(tweet_text),
                        'tweet_main_info:tweet_topic': str(tweet_topic),
                        'tweet_main_info:retweeted_flag': str(retweeted_flag),
                        'tweet_main_info:sensitive_tweet_flag': str(sensitive_tweet_flag)}
                       )

            users.put(str(user_id),
                      {'user_main_info:user_name': str(user_name),
                       'user_main_info:user_screen_name': str(user_screen_name),
                       'user_main_info:followers_count': str(followers_count),
                       'user_main_info:friends_count': str(friends_count),
                       'user_main_info:statuses_count': str(statuses_count),
                       'user_main_info:account_create_datetime': str(account_create_datetime)}
                      )
        except:
            log_error(i)
            pass

if __name__ == '__main__':
    #seperate RDDs from the Stream
    parsed.foreachRDD(write_to_db)

    #starting stream
    ssc.start()
    print('---------------------------')
    print('|Writing to Hbase started.|')
    print('---------------------------')
    ssc.awaitTermination()

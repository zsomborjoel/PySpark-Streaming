#current date (yyyy.mm.dd.)
import time
date = time.strftime("%Y%m%d")
currtime = time.strftime("%X")

#twitter API
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
import twitter_credentials as tc

#kafka API
from pykafka import KafkaClient

class TweetListener(StreamListener):

    #setting Kafka CLient and topic
    def __init__(self):
        self.client = KafkaClient(hosts="127.0.0.1:9092")
        self.topic = self.client.topics['twitter_data']

    def log_error(e):
        error_log = open("tw_stream_data_producer_error_log" + date + ".txt", "a+")
        error_log.write(str(currtime) + ' - Streaming error happened: ' + str(e) + '\n')
        error_log.close()

    #sending incoming data to produer and to file
    def on_data(self, data):
        try:
            """
            with open('twitter.json', 'a+') as myfile:
                myfile.write(data)
            """
            with self.topic.get_sync_producer() as producer:
                producer.produce(bytes(data, 'ascii'))
            return True

        except KeyError:
            return True

    #streaming error handling
    def on_error(self, status):
        self.log_error(status)
        return True

        #stop streaming on too many requests to prevent queue
        if status == 420:
            self.log_error(status)
            return False


if __name__ == '__main__':
    while True:
        try:
            #twitter credentials
            listener = TweetListener()
            auth = OAuthHandler(tc.api_key, tc.api_secret_key)
            auth.set_access_token(tc.access_token, tc.access_sectret_token)

            print('-------------------')
            print('|Producer started.|')
            print('-------------------')
            stream = Stream(auth, listener)

            #filtering twitter stream on specific language and word
            stream.filter(languages=['en'], track=['data'])
        except:
            pass
        else:
            break

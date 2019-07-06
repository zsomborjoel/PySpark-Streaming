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

from urllib3.exceptions import ProtocolError

class TweetListener(StreamListener):

    #setting Kafka CLient and topic
    def __init__(self):
        self.client = KafkaClient(hosts="127.0.0.1:9092")
        self.topic = self.client.topics['Twitter']

    #sending incoming data to produer and to file
    def on_data(self, data):
        try:
            with open('twitter.json', 'a+') as myfile:
                myfile.write(data)
            with self.topic.get_sync_producer() as producer:
                producer.produce(bytes(data, 'ascii'))
            return True

        except KeyError:
            return True

    #streaming error handling
    def on_error(self, status):
        twitter_errorlog = open("twitter_errorlog_" + date + ".txt", "a+")
        twitter_errorlog.write(str(currtime) + ' - Twitter streaming error happened - code: ' + str(status) + '\n')
        twitter_errorlog.close()
        return True

        #stop streaming on too many requests to prevent queue
        if status == 420:
            return False

if __name__ == '__main__':
    try:
        #twitter credentials
        listener = TweetListener()
        auth = OAuthHandler(tc.api_key, tc.api_secret_key)
        auth.set_access_token(tc.access_token, tc.access_sectret_token)



        print('Producer started.')
        stream = Stream(auth, listener)

        #filtering twitter stream on specific language and word
        stream.filter(languages=['en'], track=['data'])
    except ProtocolError:
        pass


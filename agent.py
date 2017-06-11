import json
import requests
import time
from dateutil.parser import parse as parsedate
from elasticsearch import Elasticsearch
from elasticsearch import helpers as eshelpers
from kafka import KafkaConsumer

#
# Constans
#
KAFKA_SERVER = 'kafka'
KAFKA_TOPIC = 'tweets'
KAFKA_CONSUMER_GROUP = 'sentiment-analysis-group'

ES_SERVER = 'elasticsearch'
ES_INDEX = 'tweets'
ES_TYPE = 'tweets'

SENTIMENT_SERVER = 'sentiment-api'
SENTIMENT_PORT = 80
SENTIMENT_ENDPOINT = 'analyze'

#
# Auxiliar functions
#

# Evaluates the sentiment of a single text
def getSentimentAnalysis(text):
    try:
        #print("Analyzing \""+text+"\"")
        response = requests.post(
            'http://%s:%s/%s' % (SENTIMENT_SERVER, SENTIMENT_PORT, SENTIMENT_ENDPOINT),
            json={'text': text}
        )
        #print("Response: "+str(response.json()))
        return response.json()['sentiment']
    except:
        print("Error analyzing text \""+text+"\"")
        return 0.0

# Process the tweet stream (returns a generator)
def processStream(tweetStream):
    for consumedObject in tweetStream:
        try:
            tweet = consumedObject.value
            # parse the creation date
            tweet['created_at'] = parsedate(tweet['created_at'])
            # analyze the sentiment of the text
            tweet['sentiment'] = getSentimentAnalysis(tweet['text'])
            yield tweet
        except:
            print("Unprocessable object:")
            print(str(tweet))

# Take tweets and format them as Elasticsearch bulk index actions
def tweets2esacttions(enrichedtweets):
    for tweet in enrichedtweets:
        yield {
            '_op_type': 'index',
            '_index': ES_INDEX,
            '_type': ES_TYPE,
            '_source': tweet
        }

#
# MAIN
#

print("Sleeping 20s to ensure kafka came up")
time.sleep(20)
print("waking up, ready to go")

# Kafka
tweets = KafkaConsumer( KAFKA_TOPIC,
                        bootstrap_servers=KAFKA_SERVER,
                        group_id=KAFKA_CONSUMER_GROUP,
                        value_deserializer=json.loads )

# ES
es = Elasticsearch(ES_SERVER)

# Create ES index if does not exist
if not es.indices.exists(ES_INDEX):
    request_body = {
        "settings" : {
            "index.mapping.total_fields.limit": 2000,
            "number_of_shards": 5,
            "number_of_replicas": 2
        }
    }
    print("creating index...")
    res = es.indices.create(index = ES_INDEX, body = request_body)

# Process tweets
actions = tweets2esacttions(processStream(tweets))
esStream = eshelpers.streaming_bulk(es,actions)
written = 0
for ok, item in esStream:
    if not ok:
        print("Error while writing element:")
        print(str(item))
    else:
        written += 1
    if written % 1000 == 0:
        print(str(written) + " tweets written")

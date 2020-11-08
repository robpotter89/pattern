# from twitter import Twitter, OAuth, Hashtag, List
import json
import re
from pprint import pprint
import os
import pandas
import tweepy as tw
import pandas as pd
from multiprocessing import Pool, Process, Lock, Queue
import time
import csv
from queue import Queue

CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""
OUTPUT = 'tweet_threads.csv'

auth = tw.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tw.API(auth, wait_on_rate_limit=True)

search_words = "election+fraud -filter:retweets"
new_q = "mail+in+ballots"
date_since = "2020-11-05"
new_search = new_q + " -filter:retweets"
fraud_search = "voter+fraud -filter:retweets"

def rem_hashtags(text, hashtag):
    processed_text = re.sub(r"#{ht}".format(ht=hashtag), "", text)
    processed_text = " ".join(processed_text.split())
    return processed_text

def remove_users(text):
    processed_text = re.sub(r'@\w+ ?',"",text)
    processed_text = " ".join(processed_text.split())
    return processed_text

def remove_links(text):
    processed_text = re.sub(r"(?:\@|http?\://|https?\://|www)\S+", "", text)
    processed_text = " ".join(processed_text.split())
    return processed_text

def remove_punct(text):
    punctuations = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    text  = "".join([char for char in text if char not in punctuations])
    text = re.sub('[0-9]+', '', text)
    return text

def lowercase_word(text):
    return "".join([char.lower() for char in text])


def clean_up_tweets(csv_file):
    data = pd.read_csv(csv_file, header = None, encoding='utf-8', names = ['Time', 'Tweets', 'Userame', 'Location'])
    data['Tweets'] = data['Tweets'].apply(lambda x:rem_hashtags(x))
    data['Tweets'] = data['Tweets'].apply(lambda x:remove_users(x))
    data['Tweets'] = data['Tweets'].apply(lambda x:remove_links(x))
    data['Tweets'] = data['Tweets'].apply(lambda x: remove_punct(x))
    data['Tweets'] = data['Tweets'].apply(lambda x: lowercase_word(x))
    return " ".join(iter(data['Tweets']))


def get_tweet_sentiment(data):
    '''
    input: cleaned tweet data
           ex. clean_up_tweets(csvfile)

    output: sentiment values

    Do sentiment analysis on the input tweets.
    '''
    if data > 0:
        return 'positive'
    elif data == 0:
        return 'neutral'
    else:
        return 'negative'
    tweets = [TextBlob(tweet) for tweet in data['Tweets']]
    data['polarity'] = [b.sentiment.polarity for b in tweets]
    data['subjectivity'] = [b.sentiment.subjectivity for b in tweets]
    data['sentiment'] = data['polarity'].apply(get_tweet_sentiment)
    return data['sentiment'].value_counts()


def make_wordcloud(data):
    '''
    input: cleaned tweet data
           ex. clean_up_tweets(csvfile)

    output: image file of wordcloud

    Create a wordcloud based on the input tweets.
    '''
    fig, ax = plt.subplots(1, 1, figsize  = (30,30))
    # Create and generate a word cloud image:
    wordcloud_ALL = WordCloud(max_font_size=50, max_words=100, background_color="white").generate(data)
    # Display the generated image:
    ax.imshow(wordcloud_ALL, interpolation='bilinear')
    ax.axis('off')


class Twittermine(object):
    """docstring for Twittermine."""
    def __init__(self, output=OUTPUT):
        super(Twittermine, self).__init__()
        self.queue = Queue()
        self.output = output
        self.uid = 0
        self.print_lock = Lock()
        self.write_lock = Lock()
        self.uid_lock = Lock()
        self.process_pool = []
        self.collect_pool = []
        self.found_ = Queue()
        self.queries = Queue()


    def fetch_tweets(self, query, item_count=100):
        return tw.Cursor(api.search,
                      q=query,
                      lang="en",
                      since=date_since).items(item_count)


    def fetch_user_loc(self, query, item_count=100):
        tweets = tw.Cursor(api.search,
                      q=query,
                      lang="en",
                      since=date_since).items(item_count)
        return [[tweet.user.screen_name, tweet.user.location] for tweet in tweets]


    def make_tweet_pandas(self, tweet_data: list, cols: list):
        return pd.DataFrame(data=tweet_data, columns=cols)


    def run(self, collect_threads=3, process_threads=3):
        '''
        Starts threads to collect tweets and threads to read them.
        '''
        print('Starting collection...')
        self.manage_collect(collect_threads)
        time.sleep(120)
        print('Starting processing...')
        self.manage_process(process_threads)
        for p in self.collect_pool:
            p.join()
        if not self.queue.empty():
            # If there's still articles to process, restart processing
            self.manage_process(process_threads)
        for p in self.process_pool:
            p.join()


    def manage_process(self, process_threads):
        '''
        Start given number of threads to multi-process tweets.
        '''
        while not self.queue.empty():
            for _ in range(process_threads):
                p = Process(target=self.process_tweets, args=())
                p.start()
                self.process_pool.append(p)
        self.print_lock.acquire()
        print('No tweets found. Ending processing.')
        self.print_lock.release()


    def manage_collect(self, collect_threads):
        '''
        Start a given number of threads to multi-process collection.
        '''
        for _ in range(collect_threads):
            p = Process(target=self.collect_tweets, args=())
            p.start()
            self.collect_pool.append(p)


    def collect_tweets(self):
        '''
        Collects tweets from sites, downloads, and adds them to queue for processing.
        '''
        while not self.queries.empty():
            search_word, items = self.queries.get()
            tweets = self.fetch_tweets(search_word, item_count=items)
            if not tweets:
                self.found_.put((search_word, items))
                self.print_lock.acquire()
                print('Error collecting tweets; moving to back of queue.')
                self.print_lock.release()
            else:
                self.queue.put(tweets)


    def process_tweets(self):
        '''
        Processes articles in queue.
        '''
        tweetlist = self.queue.get()
        print("Tweets successfully found in queue!")
        try:
            row = self.read_tweets(tweetlist)
            self.write_to_file(row, self.output)
            info = clean_up_tweets(self.output)
            make_wordcloud(info)
        except Exception as e:
            print('Error downloading or reading tweet.')
            print(e)


    def write_to_file(self, row, output, method='a'):
        '''
        Writes result to file.
        '''
        if not os.path.isfile(output) and method == 'a':
            self.write_to_file(['Time', 'Tweets', 'Userame', 'Location'], output, 'w')
        self.write_lock.acquire()
        with open(output, method) as f:
            writer = csv.writer(f)
            writer.writerow(row)
        self.write_lock.release()


    def get_uid(self):
        '''
        Gets a uid for the tweet.
        '''
        self.uid_lock.acquire()
        uid = self.uid
        self.uid += 1
        self.uid_lock.release()
        return uid


    def read_tweets(self, tweets):
        '''
        Parses tweets. Returns row of information for csv ingestion.
        '''
        print(f"Outputting tweets for search thread")
        return [[tweet.created_at, tweet.text, tweet.user.screen_name, tweet.user.location] for tweet in tweets]


    def output_data_as_df(self, all_data):
        return self.make_tweet_pandas(
            all_data, cols=['Time', 'Tweets', 'Userame', 'Location']
        )


tweeter = Twittermine()
print(tweeter.run())

#sentiment_analyzer.py
#Jack Gould
#Performs sentiment analysis on a given dataframe of articles

from nltk.sentiment import SentimentIntensityAnalyzer
import pandas as pd

def news_json_to_df(news_json):
    news_articles_df = pd.DataFrame(columns=['id', 'title', 'description', 'url', 'author', 'published'])
    for index, article in enumerate(news_json):
        news_articles_df.loc[index]=[article['id'], article['title'], article['description'], article['url'], article['author'], article['published']]
    return news_articles_df

def create_analysis_from_api_response(news):
    news_articles_df = news_json_to_df(news)
    vader_sa_df = pd.DataFrame(analyzer(news_articles_df))
    vader_sa_df = vader_sa_df.T.reset_index().rename(columns={"index" : "id"})
    total_df = vader_sa_df.merge(news_articles_df, on='id')
    return total_df
    
def analyzer(input_df):
    sia = SentimentIntensityAnalyzer()
    res = {}
    for _, row in input_df.iterrows():
        text = row['title']
        id_ = row['id']
        scores = sia.polarity_scores(text)
        res[id_] = {
            'vader_neg': scores['neg'],
            'vader_neu': scores['neu'],
            'vader_pos': scores['pos'],
            'vader_compound': scores['compound']
        }
    return res

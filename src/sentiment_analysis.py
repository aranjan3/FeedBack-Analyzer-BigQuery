from textblob import TextBlob
import pandas as pd

def analyze_sentiment(df):
    # Function to determine sentiment polarity
    def get_sentiment(text):
        return TextBlob(text).sentiment.polarity

    # Apply sentiment analysis on the 'Feedback' column
    df["sentiment"] = df["Feedback"].apply(get_sentiment)

    # Classify sentiment into Positive, Negative, Neutral
    df["sentiment_label"] = df["sentiment"].apply(lambda x: "Positive" if x > 0 else "Negative" if x < 0 else "Neutral")

    return df

# Uncomment below to test the function with a DataFrame
# df = pd.read_csv('data/feedback_backup.csv')  # If you are using local CSV, load it here
# df = analyze_sentiment(df)
# print(df.head())
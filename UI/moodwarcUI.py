import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime

def load_data_by_time(db_path, start_time, end_time):
    conn = sqlite3.connect(db_path)
    query = """
        SELECT url, category, sentiment, score, url_Timestamp as timestamp 
        FROM news_articles 
        WHERE url_Timestamp >= ? AND url_Timestamp <= ?
    """
    df = pd.read_sql_query(query, conn, params=(start_time, end_time))
    conn.close()
    return df

def load_data_by_category(db_path, category):
    conn = sqlite3.connect(db_path)
    query = """
        SELECT url, category, sentiment, score, url_Timestamp as timestamp 
        FROM news_articles 
        WHERE category = ?
    """
    df = pd.read_sql_query(query, conn, params=(category,))
    conn.close()
    return df

# Streamlit application
st.title("News Sentiment Analysis from SQLite Database")

db_path = 'moodwarcDB.db'  # Replace with the actual path to your database

# Option to filter by category or time
filter_option = st.selectbox("Choose Filter Type", ["Filter by Time", "Filter by Category"])

if filter_option == "Filter by Time":
    start_date = st.date_input("Start Date", value=datetime(2023, 9, 26))
    start_time = st.time_input("Start Time", value=datetime(2024, 9, 26, 21, 17, 51).time())

    end_date = st.date_input("End Date", value=datetime(2016, 9, 26))
    end_time = st.time_input("End Time", value=datetime(2016, 9, 26, 21, 18, 48).time())

    try:
        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)

        df = load_data_by_time(db_path, start_datetime, end_datetime)

        if df.empty:
            st.write("No records found.")
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            fig = px.scatter(df, x='timestamp', y='score',
                            color='sentiment', hover_name='url',
                            hover_data={'url': True, 'score': True},  # Show URL and score on hover
                            title='Positive and Negative News Over Time',
                            labels={'timestamp': 'Time', 'score': 'News Sentiment Score'},
                            range_y=[-1, 2])

            fig.update_traces(mode='lines+markers')
            st.plotly_chart(fig)

    except ValueError as e:
        st.write(f"Error parsing datetime: {e}")

elif filter_option == "Filter by Category":
    conn = sqlite3.connect(db_path)
    categories = pd.read_sql_query("SELECT DISTINCT category FROM news_articles", conn)
    conn.close()

    selected_category = st.selectbox("Select Category", categories['category'].tolist())

    df = load_data_by_category(db_path, selected_category)

    if df.empty:
        st.write("No records found.")
    else:
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        fig = px.scatter(df, x='timestamp', y='score',
                        color='sentiment', hover_name='url',
                        hover_data={'url': True, 'score': True},  # Show URL and score on hover
                        title=f'Positive and Negative News for Category: {selected_category}',
                        labels={'timestamp': 'Time', 'score': 'News Sentiment Score'},
                        range_y=[-1, 2])

        fig.update_traces(mode='lines+markers')
        st.plotly_chart(fig)

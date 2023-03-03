"""
# Data visualization of twitter panel search

"""

import streamlit as st
import pandas as pd
import requests

st.title('Twitter Panel Dashboard')

# write a function to take a keyword input
keyword_query = st.text_input('Search for keyword', 'optimus prime')

# use a button to change aggregate time period
aggregate_time_period = st.selectbox(
    'Aggregate time based on:',
    ('day','week','month'))

# Create a text element and let the reader know the data is loading.
data_load_state = st.text('Loading data...')

# get data for this keyword query based on aggregation period
# st.cache_data allow us to cache data without reloading it each time the page refreshes
@st.cache_data
def get_data_from_api(keyword_query, aggregate_time_period):
    z = requests.get(
        "http://achtung16:5010/keyword_search",
        json={"keyword_query": keyword_query, "aggregate_time_period": aggregate_time_period},
    )
    
    response = z.json()
    df = pd.DataFrame(response['response_data'])
    
    return df 

# present the data in a table
df = get_data_from_api(keyword_query, aggregate_time_period)

# Notify the reader that the data was successfully loaded.
data_load_state.text('Loading data...done!')

# set time as index
df['ts'] = pd.to_datetime(df['ts'])
df['date'] = [d.date() for d in df['ts']]
df = df.set_index(df['ts'])

#-----------------------------------
# Tables
# present tables of each variable
#-----------------------------------

# raw data 
st.dataframe(df)

# time series of tweet number
# n_tweets
# https://docs.streamlit.io/library/api-reference/charts/st.altair_chart

# age
st.dataframe(df['vb_age_decade'])

# gender
st.dataframe(df['voterbase_gender'])



#---------------
# Download data
#---------------
@st.cache_data
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

csv = convert_df(df)

st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name=keyword_query+'.csv',
    mime='text/csv',
)

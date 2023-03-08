"""
# Data visualization of twitter panel search

"""

import streamlit as st
import pandas as pd
import requests
import altair as alt

alt.themes.enable("streamlit")

st.title('Twitter Panel Dashboard')

# write a function to take a keyword input
keyword_query = st.text_input('Search for keyword', 'optimus prime')

# use a button to change aggregate time period
aggregate_time_period = st.selectbox(
    'Aggregate time based on:',
    ('day','week','month'))

# Create a text element and let the reader know the data is loading.
data_load_state = st.text('Loading data...')

#------------------------------------
# Function for getting data
#------------------------------------
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

# this function takes a dataframe and a variable string,
# returns a df of the variable with date as index
@st.cache_data
def get_variable_table(df, dem_variable):
    new_df = pd.DataFrame.from_records(df[dem_variable])
    new_df.set_index(df['date'], inplace = True)
    return new_df

# present the data in a table
df = get_data_from_api(keyword_query, aggregate_time_period)

# Notify the reader that the data was successfully loaded.
data_load_state.text('Loading data...done!')

# set time as index
df['ts'] = pd.to_datetime(df['ts'])
df['date'] = [d.date() for d in df['ts']]
df = df.set_index(df['ts'])


#------------------------------------
# Function for getting charts
#------------------------------------
# Define the base time-series chart.
def get_chart(data):
    hover = alt.selection_single(
        fields=["date"],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    lines = (
        alt.Chart(data, title="Number of tweets per " + aggregate_time_period)
        .mark_line()
        .encode(
            alt.X('date', title='Date'),
            alt.Y('n_tweets', title='Number of tweets')
        )
    )

    # Draw points on the line, and highlight based on selection
    points = lines.transform_filter(hover).mark_circle(size=65)

    # Draw a rule at the location of the selection
    tooltips = (
        alt.Chart(data)
        .mark_rule()
        .encode(
            x="date",
            y="n_tweets",
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip("date", title="Date"),
                alt.Tooltip("n_tweets", title="Number of tweets"),
            ],
        )
        .add_selection(hover)
    )
    return (lines + points + tooltips).interactive()

# can choose between radial or pie chart
def get_variable_chart(data, variable, type='radial'):
    data.drop(labels='Unknown', axis=1, inplace=True)
    stats = data.describe()
    new_df = pd.DataFrame({variable: stats.columns.values, 'mean': stats.loc['mean']})
    c = None
    
    if type == 'radial':
        
        base = alt.Chart(new_df).encode(
                theta=alt.Theta("mean:Q", stack=True),
                radius=alt.Radius("mean", scale=alt.Scale(type="sqrt", zero=True, rangeMin=20)),
                color=variable+":N",
                )
        c1 = base.mark_arc(innerRadius=20, stroke="#fff")
        text = base.mark_text(radiusOffset=15, size=15).encode(text=variable+":N")
        c = c1 + text
        
    elif type == 'pie':
        
        base = alt.Chart(new_df).encode(
            theta=alt.Theta("mean:Q", stack=True), color=alt.Color(variable+":N")
        )

        c1 = base.mark_arc(outerRadius=120)
        text = base.mark_text(radius=150, size=15).encode(text=variable+":N")
        
        c = c1 + text
        
    return c

#-----------------------------------
# Visualizations
#-----------------------------------

# raw data table
st.dataframe(df)

# time series of tweet number
# n_tweets
chart = get_chart(df)
st.altair_chart(chart.interactive(), use_container_width=True)

# age
st.dataframe(get_variable_table(df, 'vb_age_decade'))

# gender
gender_df = get_variable_table(df, 'voterbase_gender')
st.dataframe(gender_df)

c = get_variable_chart(gender_df, 'gender','pie')
st.altair_chart(c, use_container_width=True)


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

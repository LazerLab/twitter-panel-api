# tweet-search-dashboard
dashboard app for searching twitter panel tweets &amp; aggregating info

## To get this to work:
- install the python packages: `pip install -r requirements.txt`
- have a working elasticsearch instance listening on port 9200 (or change the port in the code as needed)
- have a port you can use for a Flask app.

** Note: on achtung06, we already have an ES instance up that is populated with some data for testing. 
It is listening on port 9200. Probably it's good to do development on achtung06 for this reason.
** 

## Steps to make this do stuff:
- Ingest Tweets into Elasticsearch
- Ingest voters into PostgreSQL
- Create a config JSON file, modifying the defaults seen in `config.py`
- Launch the Flask app (`CONFIG=/path/to/config.json flask run --port 5010 --host 0.0.0.0`)
- ssh into achtung: `ssh -L localhost:5010:achtung06:5010 $username@achtung.ccs.neu.edu`
- Submit queries
- If you want to debug the app, you'll have to take it down (ctrl-c) and relaunch (see above). Should go down & come back up pretty fast.

## Querying the Data
Currently, only the `/keyword_search` endpoint is functioning.

`/keyword_search`:

Parameters:
- keyword_query: string
- aggregate_time_period: string (day|week|month)


### Example
```
GET /keyword_search
{
  "keyword_query": "dinosaur",
  "aggregate_time_period": "week"
}
```
```
Response:
{
  "query": {
    "keyword_query": "dinosaur",
    "aggregate_time_period": "week"
  }
  "response_data": [
    {
      "n_tweeters": 1025,
      "n_tweets": 110,
      "ts": "Mon, 28 Sep 2020 00:00:00 GMT",
      "tsmart_state": {
        "AK": 1,
        "AL": 3,
        ...
      },
      "vb_age_decade": {
        "10 - 20": 12,
        "20 - 30": 400,
        ...
        "unknown": 10
      },
      "voterbase_gender": {
        "Female": 400,
        "Male": 400,
        "Unknown": 225
      },
      "voterbase_race": {
        "African-American": 100,
        "Asian": 100,
        "Caucasian": 100,
        "Hispanic": 100,
        "Native American": 100,
        "Other": 100,
        "Uncoded": 425
      }
    },
    ...
  ]
}
```

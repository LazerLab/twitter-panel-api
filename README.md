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
- Ingest data from Spark or a .TSV; put it into ES (use the function `port_table_to_elastic()` in `utils.py`)
- Check that search works outside Flask (test w/ (e.g.) `elastic_query_for_keyword('dog')`. 
- Launch the Flask app (`flask run --port 5010 --host 0.0.0.0`)
- ssh into achtung: `ssh -L localhost:5010:achtung06:5010 $username@achtung.ccs.neu.edu`
- In your browser, w/ a proxy installed and running so that the localhost port will show up in your browser, go to localhost:5150.
- The site should show up and you should be able to query it. If that isn't the case, look at the terminal window where the Flask app is running. It's probably displaying error messages.
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
  "agg_time_period": "week",
  "query": "dinosaur",
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

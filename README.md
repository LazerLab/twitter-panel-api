[![Linting](https://github.com/hdelc/tweet-search-dashboard/actions/workflows/lint.yml/badge.svg)](https://github.com/hdelc/tweet-search-dashboard/actions/workflows/lint.yml)
# tweet-search-dashboard
dashboard app for searching twitter panel tweets &amp; aggregating info

## To get this to work:
- install dependencies: `pip install -r requirements.txt`
- install this package: `pip install .`
- have a working elasticsearch instance for Twitter data listening at `ELASTICSEARCH_URL` in the config file
- have a working postgresql database for demographic data listening at `DATABASE_URL` in the config file
- have a port you can use for a Flask app.

## Steps to make this do stuff:
- Ingest Tweets into Elasticsearch
- Ingest voters into PostgreSQL
- Create a config JSON file, modifying the defaults seen in `panel_api/__init__.py`
- Launch the Flask app (`API_CONFIG=/path/to/config.json gunicorn --bind 127.0.0.1:8000 'panel_api:create_app()'`)
- Submit queries

## Querying the Data
Currently, only the `/keyword_search` endpoint is available.

`/keyword_search`:

Parameters:
- (required) keyword_query: string
- (required) aggregate_time_period: string (day|week|month)
- (optional) cross_sections: array
  - type: string (age|race|gender|state)
- (optional) before: string (ISO 8601 date string)
- (optional) after: string (ISO 8601 date string)


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
      "ts": "2020-09-28T00:00:00.000",
      "tsmart_state": {
        "AK": 1,
        "AL": 3,
        ...
      },
      "vb_age_decade": {
        "under 30": 12,
        "30 - 40": 400,
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

# Contributing

This repository uses the "Squash & Merge" strategy to merge pull requests with the `main` branch. Because of this, please make sure your pull requests only make one self-contained change, since any commits in the pull request will not be able to be teased apart once they are merged.

All pull requests to this repository must pass a few automated checks before they may be accepted:

- Linting
  - Ensure `make lint` passes for your code. This runs style checks and static type checks. Formatting issues can be solved with `make format`, but missing documentation and improper types must be manually handled. I'd recommend putting `make lint` in a Git hook, so your pull requests aren't denied.
- Testing
  - Ensure `make test` passes for your code. At present, this just runs the existing unit tests. If you add/change functionality, it would be wonderful if you also add tests for that in the same PR. Also, I recommend putting `make test` in a Git hook.
- Conventional PR Title
  - When creating a PR, write the title to follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/#summary) format. This helps readers understand what the PR is doing and keeps the commit history tidy. This is also a strict requirement because these commit labels allow automatic semantic version bumps.

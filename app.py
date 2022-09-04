from flask import Flask, render_template, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
import pandas as pd

from .utils import elastic_query_for_keyword, elastic_query_users

app = Flask(__name__)
lines = []
with open('flask_secret_key.txt', 'r') as f:
    for line in f.readlines();
        lines.append(line.strip())

app.config["SECRET_KEY"] = lines[0]
Bootstrap(app)


def int_or_nan(b):
    """
    hopefully b is a string that converts nicely to an int.
    if it is not, we return 0.
    a sketchy way to coerce strings to ints.
    i'm just using this for user IDs (need a better solution for prod)
    """
    try:
        return int(b)
    except:
        return 0


class QueryESForm(FlaskForm):
    """
    Flask form to submit a query. Has to have data in it. Has a Submit button too.
    """

    query = StringField(
        "Enter your query here (boolean stuff does not work yet!!)",
        validators=[DataRequired()],
    )
    submit = SubmitField("Submit")


@app.route("/", methods=["GET", "POST"])
def landing_page():
    """
    Makes the landing page (and, currently, only page)
    for querying the ES-indexed portion of the twitter panel and their tweets
    """
    form = QueryESForm()
    message = "hi"
    # hopefully we have data
    if form.validate_on_submit():
        query = form.query.data
        # the query, in plaintext, is what we pop into the query
        res = elastic_query_for_keyword(query)
        # querying ES for the query (no booleans or whatever exist yet!!)
        results = []
        # we make a dataframe out of the results.
        for hit in res:
            results.append(hit.to_dict())

        # gotta apologize if we came up empty
        if len(res) == 0:
            message = "sorry - no results found"
        else:
            res_df = pd.DataFrame(results)
            # otherwise we make a dataframe

        # grab all the users who tweeted
        users = elastic_query_users(res_df["userid"])
        users_df = pd.DataFrame(users)

        # coerce user IDs
        res_df["userid"] = res_df["userid"].apply(lambda b: int_or_nan(b))
        users_df["twProfileID"] = users_df["twProfileID"].apply(lambda b: int_or_nan(b))
        # need them to both be ints to do the join
        # join results with the user demographics by twitter user ID
        full_df = res_df.merge(users_df, left_on="userid", right_on="twProfileID")

        # right now we're aggregating results by day. this can change later.
        full_df["day_rounded"] = pd.to_datetime(full_df["timestamp"]).dt.floor("d")
        # bucket age by decade
        full_df["vb_age_decade"] = full_df["voterbase_age"].apply(
            lambda b: str(10 * int(b / 10)) + " - " + str(10 + 10 * int(b / 10))
        )
        # aggregate by day
        table = full_df.groupby("day_rounded")
        # get all value counts for each day
        result_df = []
        for ts, t in table:
            t_dict = {"ts": ts, "n_tweets": len(t), "n_tweeters": len(set(t["userid"]))}
            for i in ["vb_age_decade", "voterbase_gender", "voterbase_race"]:
                t_dict.update(t[i].value_counts().to_dict())
            result_df.append(t_dict)
        # make df with value counts.
        # each row is a day's aggregation of tweets plus the demographics of the ppl who tweeted.
        # also the number of tweets.
        result_df = pd.DataFrame(result_df)
        tables = [result_df.to_html(classes="data")]
        return render_template(
            "result.html",
            tables=tables,
            titles=result_df.columns.values,
            bigtitle="your data, buddy; keyword={}".format(query),
        )
    else:
        message = "sorry, but you didn't enter a query"
    return render_template("index.html", form=form, message=message)

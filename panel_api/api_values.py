"""Define and encode constants used by the API."""

from enum import Enum


class Demographic(str, Enum):
    """
    Demographics that can be queried in the API.
    """

    STATE = "tsmart_state"
    AGE = "vb_age_decade"
    GENDER = "voterbase_gender"
    RACE = "voterbase_race"

    def __str__(self):
        return self.value

    def values(self):
        """
        Return a list of all possible API values for a demographic.
        """
        return DEMOGRAPHIC_VALUES[self]


class TimeAggregation(str, Enum):
    """
    Valid time aggregation slices queryable by the API.
    """

    DAY = "day"
    WEEK = "week"
    MONTH = "month"

    def __str__(self):
        return self.value

    def round_key(self):
        """
        Return the Pandas time period rounding key for this aggregation.
        """
        return AGG_TO_ROUND_KEY[self]


AGG_TO_ROUND_KEY = {
    TimeAggregation.DAY: "D",
    TimeAggregation.WEEK: "W",
    TimeAggregation.MONTH: "M",
}

DEMOGRAPHIC_VALUES = {
    Demographic.RACE: [
        "Caucasian",
        "African-American",
        "Hispanic",
        "Uncoded",
        "Asian",
        "Other",
        "Native American",
    ],
    Demographic.AGE: [
        "10 - 20",
        "20 - 30",
        "30 - 40",
        "40 - 50",
        "50 - 60",
        "60 - 70",
        "70 - 80",
        "80 - 90",
        "90 - 100",
        "100 - 110",
        "110 - 120",
        "120 - 130",
        "130 - 140",
        "140 - 150",
    ],
    Demographic.GENDER: ["Female", "Male", "Unknown"],
    Demographic.STATE: [
        "CA",
        "TX",
        "NY",
        "FL",
        "OH",
        "IL",
        "PA",
        "MI",
        "GA",
        "NC",
        "WA",
        "MA",
        "MN",
        "NJ",
        "IN",
        "VA",
        "CO",
        "WI",
        "TN",
        "AZ",
        "MO",
        "OR",
        "MD",
        "IA",
        "KY",
        "LA",
        "AL",
        "SC",
        "OK",
        "KS",
        "CT",
        "NV",
        "NE",
        "AR",
        "UT",
        "MS",
        "DC",
        "WV",
        "ME",
        "NM",
        "NH",
        "RI",
        "ID",
        "HI",
        "SD",
        "MT",
        "ND",
        "DE",
        "AK",
        "VT",
        "WY",
    ],
}

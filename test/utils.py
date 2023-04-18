import pandas as pd

from panel_api.api_values import Demographic


def period_equals(period1, period2):
    if set(period1.keys()) != set(period2.keys()):
        return False
    for dem in Demographic:
        if not all(pd.Series(period1[dem]).eq(pd.Series(period2[dem]), fill_value=0)):
            return False
    if "groups" in period1:
        p1_groups = pd.DataFrame(period1["groups"])
        p1_cross_sections = set(col for col in p1_groups.columns if col != "count")
        p2_groups = pd.DataFrame(period2["groups"])
        p2_cross_sections = set(col for col in p2_groups.columns if col != "count")
        if p1_cross_sections != p2_cross_sections:
            return False
        cross_sections = list(p1_cross_sections)
        return all(
            p1_groups.set_index(cross_sections)["count"].eq(
                p2_groups.set_index(cross_sections)["count"], fill_value=0
            )
        )

    return True


def list_equals_ignore_order(list1, list2, item_equality=None):
    if item_equality is None:

        def item_equality(a, b):
            return a == b

    return (
        len(list1) == len(list2)
        and all(
            [any([item_equality(item1, item2) for item1 in list1]) for item2 in list2]
        )
        and all(
            [any([item_equality(item1, item2) for item2 in list2]) for item1 in list1]
        )
    )


def iterator_hook(iterator, hook):
    for item in iterator:
        hook(item)
        yield item

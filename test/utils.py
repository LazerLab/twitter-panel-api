def period_equals(period1, period2):
    if set(period1.keys()) != set(period2.keys()):
        return False
    for key in set(period1.keys()):
        if key != "groups" and period1[key] != period2[key]:
            return False
    if "groups" in period1:
        return list_equals_ignore_order(period1["groups"], period2["groups"])

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

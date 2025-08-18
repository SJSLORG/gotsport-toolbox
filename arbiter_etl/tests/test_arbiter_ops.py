import pandas as pd

from ..arbiter_ops import ArbiterOperations


def make_row(d):
    return pd.Series(d)


def test_custom_replace_both_present_prefix_match():
    ops = ArbiterOperations('.', '.')
    row = make_row({'Home Club': 'ALPHA FC', 'Home Team': 'ALPHA U12'})
    assert ops.custom_replace(row, 'Home') == 'ALPHA U12'


def test_custom_replace_both_present_no_prefix_match():
    ops = ArbiterOperations('.', '.')
    row = make_row({'Home Club': 'BRAVO CLUB', 'Home Team': 'UNITED'})
    assert ops.custom_replace(row, 'Home') == 'BRAVO CLUB UNITED'


def test_custom_replace_missing_team():
    ops = ArbiterOperations('.', '.')
    row = make_row({'Home Club': 'CHARLIE', 'Home Team': None})
    assert ops.custom_replace(row, 'Home') == 'CHARLIE'


def test_custom_replace_missing_club():
    ops = ArbiterOperations('.', '.')
    row = make_row({'Home Club': None, 'Home Team': 'DRAGONS'})
    assert ops.custom_replace(row, 'Home') == 'DRAGONS'


def test_custom_replace_both_missing():
    ops = ArbiterOperations('.', '.')
    row = make_row({'Home Club': None, 'Home Team': None})
    assert ops.custom_replace(row, 'Home') == ''

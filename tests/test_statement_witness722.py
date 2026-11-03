from copy import deepcopy
import pytest
from pipeline.statement_witness722 import PROFILES, validate_statement, source_witness


def fixture():
    profile = deepcopy(PROFILES['NTAP'])
    rows = [['Year Ended'], ['April 26, 2019', 'April 27, 2018', 'April 28, 2017'],
            ['(In millions, except per share amounts)'], [], [], [], [],
            ['Net revenues', '6146', '5919', '5491']]
    return rows, 'NetApp, Inc. Consolidated Statements of Operations', profile


def test_primary_statement_witness_never_approves_or_inherits_adjustment_units():
    rows, context, p = fixture()
    before = deepcopy((rows, p))
    r = validate_statement(rows, context, p)
    assert r[1]['primaryStatementUsd'] == '5919000000'
    assert all(v['primaryStatementUnitBound'] and not v['adjustmentScheduleUnitBound']
               and not v['publicationAllowed'] and not v['approvalGenerated']
               and not v['periodStartBound'] for v in r)
    assert (rows, p) == before


@pytest.mark.parametrize('kind', ['company', 'title', 'annual', 'dates', 'unit', 'unit_conflict',
                                 'value', 'comparative', 'extra_column', 'row_label'])
def test_unrelated_or_changed_statement_must_not_supply_a_unit_witness(kind):
    rows, context, p = fixture()
    if kind == 'company': context = context.replace('NetApp', 'Another')
    if kind == 'title': context = context.replace('Statements of Operations', 'Statements of Cash Flows')
    if kind == 'annual': rows[0] = ['Quarter Ended']
    if kind == 'dates': rows[1][1] = 'April 28, 2018'
    if kind == 'unit': rows[2] = ['in thousands']
    if kind == 'unit_conflict': rows[2].append('in thousands')
    if kind == 'value': rows[7][2] = '5918'
    if kind == 'comparative': rows[7][1] = '6145'
    if kind == 'extra_column': rows[7].append('1')
    if kind == 'row_label': rows[7][0] = 'Revenue growth'
    with pytest.raises(ValueError): validate_statement(rows, context, p)


def test_other_document_cannot_borrow_pinned_source_identity():
    with pytest.raises(ValueError, match='SOURCE_CHANGED'): source_witness(b'<html/>', 'NTAP')

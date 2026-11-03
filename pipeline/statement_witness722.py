"""Local-only primary statement witness; never transfers units to another table.

Matching an adjusted value in a primary statement does not establish the unit
of its separate adjustment schedule, nor a complete reconciliation contract.
"""
import hashlib
import re
from datetime import datetime
from decimal import Decimal

from pipeline.legacy_statement_total699 import clean, DATE
from pipeline.reconciliation_review679 import Tables, numeric
from pipeline.reviewed_financial_mapping672 import digest

PROFILES = {
    'NTAP': dict(sha='56e1c1d6d32cd92c66ca2ac3792619fbbeba8690f0987842371f6443be062f22',
                 table='d53b4846c320fc6cb005708b0fe51f928d7d33507fe1bf29014c71f323290231',
                 entity='netapp, inc.', unit='millions', row=7,
                 ends=['2019-04-26', '2018-04-27', '2017-04-28'],
                 values=['6146', '5919', '5491']),
    'EXTR': dict(sha='c72134b231b8e31b92b4b88c919614fdb331a8326d0f47eb80cad9946305fe13',
                 table='abffc609eda0eacd744c72ff41cd824856f27a8b8f6b05f9e8bc3840b30b0965',
                 entity='extreme networks, inc.', unit='thousands', row=6,
                 ends=['2018-06-30', '2017-06-30', '2016-06-30'],
                 values=['983142', '607084', '519834']),
}


def validate_statement(rows, context, profile):
    """Bind only this table's entity, annual columns, printed unit and totals."""
    ctx = clean(context)
    if not re.search(re.escape(profile['entity']) + r'\s+consolidated statements of operations(?:\s|$)', ctx):
        raise ValueError('STATEMENT_WITNESS_ENTITY_TITLE')
    if clean(' '.join(rows[0])) != 'year ended':
        raise ValueError('STATEMENT_WITNESS_ANNUAL_HEADER')
    dates = [datetime.strptime(' '.join(m.groups()), '%B %d %Y').date().isoformat()
             for m in DATE.finditer(' '.join(rows[1]))]
    if dates != profile['ends']:
        raise ValueError('STATEMENT_WITNESS_DATE_COLUMNS')
    # Only this exact statement heading and its header rows supply the unit.
    heading = ctx[ctx.rfind(profile['entity']):]
    units = set(re.findall(r'\bin (thousands|millions)\b', heading + ' ' + clean(' '.join(' '.join(r) for r in rows[:3]))))
    if units != {profile['unit']}:
        raise ValueError('STATEMENT_WITNESS_PRINTED_UNIT')
    row = rows[profile['row']]
    if clean(row[0]) not in ('net revenues', 'total net revenues'):
        raise ValueError('STATEMENT_WITNESS_TOTAL_ROW')
    values = [n for cell in row[1:] if (n := numeric(cell)) is not None]
    if values != [Decimal(v) for v in profile['values']]:
        raise ValueError('STATEMENT_WITNESS_ALL_COMPARATIVES')
    scale = Decimal(1000 if profile['unit'] == 'thousands' else 1000000)
    return [dict(end=end, printedValue=str(value), primaryStatementUsd=str(value * scale),
                 primaryStatementUnitBound=True, adjustmentScheduleUnitBound=False,
                 periodStartBound=False, approvalGenerated=False, publicationAllowed=False)
            for end, value in zip(dates, values)]


def source_witness(body, ticker):
    profile = PROFILES[ticker]
    if len(body) > 32 * 1024 ** 2 or hashlib.sha256(body).hexdigest() != profile['sha']:
        raise ValueError('STATEMENT_WITNESS_SOURCE_CHANGED')
    parser = Tables()
    parser.feed(body.decode('utf8', 'replace'))
    found = [(rows, context) for rows, context in zip(parser.tables, parser.contexts)
             if digest(rows) == profile['table']]
    if len(found) != 1:
        raise ValueError('STATEMENT_WITNESS_EXACT_TABLE')
    rows, context = found[0]
    return dict(ticker=ticker, documentSha256=profile['sha'], tableHash=profile['table'],
                rows=validate_statement(rows, context, profile),
                reviewStillRequired=['EXPLICIT_ADJUSTMENT_SCHEDULE_UNIT_SCOPE',
                                     'ACTUAL_PERIOD_START', 'ALL_ORIGINAL_AND_LATEST_REPORTS',
                                     'FRESH_DATABASE_AND_SERVING_VALIDATION'],
                approvalGenerated=False, publicationAllowed=False)

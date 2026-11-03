"""Reproducible local-only residual report; no database or network access.

Run against a frozen ledger and an independently audited resolved-key index.
The report is diagnostic and cannot be submitted as an approval manifest.
"""
import argparse
from collections import Counter
from datetime import date
import gzip
import hashlib
import json
from pathlib import Path
from pipeline.residual_diagnostics705 import diagnose


def key(row):
    return (row['companyId'], row['metricId'], row['end'])


def report(ledger, resolved_path, as_of, maximum=200000):
    resolved_rows = json.loads(resolved_path.read_text(encoding='utf8'))
    resolved = {key(r) for r in resolved_rows}
    if len(resolved) != len(resolved_rows):
        raise ValueError('DUPLICATE_RESOLVED_KEY')
    seen, matched = set(), set()
    reasons, diagnostics, flags = Counter(), Counter(), Counter()
    frozen_scopes, frozen_observations = Counter(), Counter()
    for line in gzip.open(ledger, 'rt', encoding='utf8'):
        scope = json.loads(line); k = key(scope)
        if k in seen:
            raise ValueError('DUPLICATE_LEDGER_KEY')
        seen.add(k)
        if len(seen) > maximum:
            raise ValueError('LEDGER_SCOPE_LIMIT')
        if k in resolved:
            matched.add(k); continue
        d = diagnose(scope, as_of)
        reasons[d['replayReason']] += 1
        diagnostics[d['diagnosticReason']] += 1
        flags.update(d['dateFlags'])
        frozen = scope.get('diagnosticFlags', {})
        if not isinstance(frozen, dict) or any(type(n) is not int or n < 0 for n in frozen.values()):
            raise ValueError('INVALID_FROZEN_DIAGNOSTIC_COUNTS')
        frozen_scopes.update(k for k, n in frozen.items() if n > 0)
        frozen_observations.update(frozen)
    if matched != resolved:
        raise ValueError('RESOLVED_KEY_NOT_IN_SOURCE_LEDGER')
    return {'contract': 'LOCAL_RESIDUAL_DIAGNOSTICS705_V1',
            'asOf': as_of.isoformat(), 'sourceScopes': len(seen),
            'auditedResolvedScopes': len(matched), 'remainingScopes': sum(reasons.values()),
            'byReason': dict(sorted(reasons.items())),
            'exclusiveDiagnostics': dict(sorted(diagnostics.items())),
            'dateFlagsScopeCountsMayOverlap': dict(sorted(flags.items())),
            'frozenDiagnosticScopeCountsMayOverlap': dict(sorted(frozen_scopes.items())),
            'frozenDiagnosticObservationCountsMayOverlap': dict(sorted(frozen_observations.items())),
            'ledgerSha256': hashlib.sha256(ledger.read_bytes()).hexdigest(),
            'resolvedIndexSha256': hashlib.sha256(resolved_path.read_bytes()).hexdigest(),
            'mappingApprovals': 0, 'databaseWrites': 0, 'sourceDownloads': 0,
            'currencyConversions': 0, 'overallResidualResolved': False}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ledger', type=Path, required=True)
    p.add_argument('--resolved-index', type=Path, required=True)
    p.add_argument('--as-of', type=date.fromisoformat, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise ValueError('OUTPUT_ALREADY_EXISTS')
    result = report(args.ledger, args.resolved_index, args.as_of)
    with args.output.open('x', encoding='utf8') as f:
        json.dump(result, f, indent=2, sort_keys=True)
    print(json.dumps(result))


if __name__ == '__main__':
    main()

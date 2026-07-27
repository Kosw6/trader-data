import argparse
import json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

from backfills.sec_company_backfill import (
    normalize_cik,
    normalize_sec_exchange,
    normalize_ticker,
    parse_sec_exchange_rows,
    require_user_agent,
)
from collectors.sec_client import SECClient


LOGGER = logging.getLogger("asset-company-match-backfill")
MATCH_METHOD = "SEC_KIS_TICKER_EXCHANGE_EXACT"


def parse_args():
    """Read matching scope and write-safety flags from the command line."""
    parser = argparse.ArgumentParser(
        description="Build SEC Company candidates for current KIS Assets."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write candidates. Without this flag only print matching statistics.",
    )
    parser.add_argument(
        "--auto-confirm-exact",
        action="store_true",
        help=(
            "Confirm one-to-one ticker+exchange matches and update asset.company_id. "
            "Requires --apply."
        ),
    )
    parser.add_argument("--asset-type", default="EQUITY")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--request-interval", type=float, default=0.2)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def require_database_url():
    """Return the PostgreSQL URL required for Asset and Company matching."""
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is required in .env or the environment")
    return value


def fetch_assets(connection, asset_type):
    """Load active Assets and their current KIS provider identities."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                a.id, a.ticker, a.name, a.exchange, a.asset_type, a.company_id,
                aps.provider_exchange_code, aps.provider_symbol
            FROM asset a
            JOIN asset_provider_symbol aps
              ON aps.asset_id = a.id
             AND aps.provider = 'KIS'
             AND aps.is_current = TRUE
            WHERE a.active = TRUE
              AND (%s = '' OR a.asset_type = %s)
            ORDER BY a.id
            """,
            (asset_type, asset_type),
        )
        return [
            {
                "asset_id": row[0],
                "ticker": normalize_ticker(row[1]),
                "asset_name": row[2],
                "exchange": row[3],
                "asset_type": row[4],
                "current_company_id": row[5],
                "provider_exchange_code": row[6],
                "provider_symbol": normalize_ticker(row[7]),
            }
            for row in cursor.fetchall()
        ]


def fetch_companies(connection):
    """Load SEC Companies and the latest Submissions source object for evidence."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.id, c.cik, c.company_name, source.id
            FROM company c
            LEFT JOIN LATERAL (
                SELECT so.id
                FROM source_object so
                WHERE so.provider = 'SEC'
                  AND so.data_type = 'SUBMISSIONS'
                  AND so.external_id = c.cik
                ORDER BY so.collected_at DESC, so.id DESC
                LIMIT 1
            ) source ON TRUE
            """
        )
        return {
            normalize_cik(row[1]): {
                "company_id": row[0],
                "cik": normalize_cik(row[1]),
                "company_name": row[2],
                "sec_source_object_id": row[3],
            }
            for row in cursor.fetchall()
        }


def build_match_candidates(sec_payload, assets, companies, auto_confirm_exact=False):
    """Join SEC and KIS identities and classify unique or ambiguous candidates."""
    stats = Counter()
    assets_by_identity = defaultdict(list)
    sec_by_identity = defaultdict(dict)

    for asset in assets:
        identity = (asset["provider_symbol"], asset["exchange"])
        assets_by_identity[identity].append(asset)
        stats["assets"] += 1

    for row in parse_sec_exchange_rows(sec_payload):
        stats["sec_rows"] += 1
        ticker = normalize_ticker(row.get("ticker"))
        exchange = normalize_sec_exchange(row.get("exchange"))
        if not ticker or exchange is None:
            stats["sec_unsupported"] += 1
            continue
        cik = normalize_cik(row.get("cik"))
        company = companies.get(cik)
        if company is None:
            stats["company_not_loaded"] += 1
            continue
        sec_by_identity[(ticker, exchange)][cik] = {
            **company,
            "sec_ticker": ticker,
            "sec_exchange": exchange,
            "sec_company_name": row.get("name") or company["company_name"],
        }

    candidates = []
    for identity in sorted(set(assets_by_identity) & set(sec_by_identity)):
        identity_assets = assets_by_identity[identity]
        identity_companies = list(sec_by_identity[identity].values())
        unique_match = len(identity_assets) == 1 and len(identity_companies) == 1

        for asset in identity_assets:
            for company in identity_companies:
                linked_elsewhere = asset["current_company_id"] not in (
                    None,
                    company["company_id"],
                )
                if unique_match and not linked_elsewhere:
                    status = "CONFIRMED" if auto_confirm_exact else "CANDIDATE"
                    confidence = 1.0
                    stats["unique_exact"] += 1
                else:
                    status = "REVIEW_REQUIRED"
                    confidence = 0.85
                    stats["review_required"] += 1

                evidence = {
                    "rule": "ticker_and_normalized_exchange_exact",
                    "ticker": identity[0],
                    "exchange": identity[1],
                    "kis": {
                        "providerSymbol": asset["provider_symbol"],
                        "providerExchangeCode": asset["provider_exchange_code"],
                        "assetName": asset["asset_name"],
                    },
                    "sec": {
                        "cik": company["cik"],
                        "ticker": company["sec_ticker"],
                        "exchange": company["sec_exchange"],
                        "companyName": company["sec_company_name"],
                    },
                    "identityAssetCount": len(identity_assets),
                    "identityCompanyCount": len(identity_companies),
                    "linkedElsewhere": linked_elsewhere,
                }
                candidates.append(
                    {
                        "asset_id": asset["asset_id"],
                        "company_id": company["company_id"],
                        "cik": company["cik"],
                        "ticker": identity[0],
                        "exchange": identity[1],
                        "status": status,
                        "match_confidence": confidence,
                        "sec_source_object_id": company["sec_source_object_id"],
                        "evidence": evidence,
                    }
                )

    stats["candidates"] = len(candidates)
    stats["assets_without_sec_match"] = sum(
        len(value)
        for identity, value in assets_by_identity.items()
        if identity not in sec_by_identity
    )
    return candidates, stats


def upsert_candidate(cursor, candidate, matched_at):
    """Upsert one candidate without overwriting manual confirmation or rejection."""
    cursor.execute(
        """
        INSERT INTO asset_company_match (
            asset_id, company_id, match_method, match_confidence, status,
            sec_source_object_id, market_source_object_id, evidence, matched_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s)
        ON CONFLICT (asset_id, company_id)
        DO UPDATE SET
            match_method = EXCLUDED.match_method,
            match_confidence = EXCLUDED.match_confidence,
            sec_source_object_id = EXCLUDED.sec_source_object_id,
            evidence = EXCLUDED.evidence,
            matched_at = EXCLUDED.matched_at,
            status = CASE
                WHEN asset_company_match.status IN ('CONFIRMED', 'REJECTED')
                    THEN asset_company_match.status
                ELSE EXCLUDED.status
            END,
            updated_at = CURRENT_TIMESTAMP
        RETURNING status
        """,
        (
            candidate["asset_id"],
            candidate["company_id"],
            MATCH_METHOD,
            candidate["match_confidence"],
            candidate["status"],
            candidate["sec_source_object_id"],
            Json(candidate["evidence"]),
            matched_at,
        ),
    )
    return cursor.fetchone()[0]


def apply_candidates(connection, candidates):
    """Persist candidates and atomically link Assets whose status is confirmed."""
    stats = Counter()
    matched_at = datetime.now(timezone.utc)
    with connection.cursor() as cursor:
        for candidate in candidates:
            cursor.execute("SAVEPOINT asset_company_candidate")
            try:
                persisted_status = upsert_candidate(cursor, candidate, matched_at)
                stats[f"persisted:{persisted_status}"] += 1
                if persisted_status == "CONFIRMED":
                    cursor.execute(
                        """
                        UPDATE asset
                        SET company_id = %s
                        WHERE id = %s
                          AND (company_id IS NULL OR company_id = %s)
                        """,
                        (
                            candidate["company_id"],
                            candidate["asset_id"],
                            candidate["company_id"],
                        ),
                    )
                    if cursor.rowcount == 1:
                        stats["assets_linked"] += 1
                    else:
                        raise ValueError(
                            f"asset {candidate['asset_id']} is linked to another company"
                        )
            except Exception as error:
                cursor.execute("ROLLBACK TO SAVEPOINT asset_company_candidate")
                stats["failed"] += 1
                LOGGER.exception(
                    "asset=%s company=%s failed: %s",
                    candidate["asset_id"],
                    candidate["company_id"],
                    error,
                )
            finally:
                cursor.execute("RELEASE SAVEPOINT asset_company_candidate")
    connection.commit()
    return stats


def main():
    """Fetch SEC identities, generate candidates, and optionally persist them."""
    load_dotenv()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if args.auto_confirm_exact and not args.apply:
        raise ValueError("--auto-confirm-exact requires --apply")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("limit must be positive")
    if args.request_interval < 0:
        raise ValueError("request-interval must not be negative")

    connection = psycopg2.connect(require_database_url())
    connection.autocommit = False
    try:
        assets = fetch_assets(connection, args.asset_type.strip().upper())
        companies = fetch_companies(connection)
        connection.rollback()

        client = SECClient(
            require_user_agent(), min_request_interval=args.request_interval
        )
        payload = client.get_company_tickers_with_exchange()
        candidates, stats = build_match_candidates(
            payload,
            assets,
            companies,
            auto_confirm_exact=args.auto_confirm_exact,
        )
        if args.limit is not None:
            candidates = candidates[: args.limit]
            stats["limited_candidates"] = len(candidates)

        apply_stats = Counter()
        if args.apply:
            apply_stats = apply_candidates(connection, candidates)
        else:
            connection.rollback()
    finally:
        connection.close()

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(
        f"mode={mode} asset_type={args.asset_type.strip().upper()} "
        f"auto_confirm_exact={args.auto_confirm_exact}"
    )
    for key in sorted(stats):
        print(f"{key}={stats[key]}")
    for key in sorted(apply_stats):
        print(f"{key}={apply_stats[key]}")
    if apply_stats["failed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()

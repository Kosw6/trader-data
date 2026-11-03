"""Strict adjusted-price candidate publication; never repairs invalid bars."""

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname

from backfills.chart_snapshot_selection import select_snapshot_bars
from collectors.common import content_hash
from collectors.kis_client import normalize_daily_price

from pipeline.durable_requests import CoordinationError, canonical_hash
from pipeline.stage_transaction import apply_phase


def load_raw(cur, work_key, allowed_root, *, locking=True):
    cur.execute(
        "SELECT sources,contract FROM pipeline_recovery_raw_intent WHERE work_key=%s",
        (work_key,),
    )
    row = cur.fetchone()
    if not row:
        raise CoordinationError("review_required")
    sources, contract = row
    loaded = []
    for source in sources:
        cur.execute(
            "SELECT object_uri,object_size,content_hash,provider,data_type FROM source_object WHERE id=%s"
            + (" FOR SHARE" if locking else ""),
            (source["id"],),
        )
        meta = cur.fetchone()
        if not meta or meta[1:3] != (source["size"], source["hash"]):
            raise CoordinationError("contract_conflict")
        uri = urlsplit(meta[0])
        path = Path(url2pathname(unquote(uri.path))).resolve()
        if (
            uri.scheme != "file"
            or uri.netloc
            or not path.is_relative_to(Path(allowed_root).resolve())
        ):
            raise CoordinationError("invalid_contract")
        raw = path.read_bytes()
        payload = json.loads(raw)
        if len(raw) != meta[1] or content_hash(payload) != meta[2]:
            raise CoordinationError("contract_conflict")
        loaded.append((source, meta[3], meta[4], payload))
    return contract, loaded


def price_content(cur, plan, allowed_root):
    contract, sources = load_raw(cur, plan["workKey"], allowed_root)
    if (
        plan["kind"] != "PRICE"
        or plan["policyVersion"] not in {"adjusted-full-v1", "adjusted-calendar-v2"}
        or len(sources) != 1
    ):
        raise CoordinationError("invalid_contract")
    _source, provider, datatype, raw = sources[0]
    asset = int(contract["target"].removeprefix("asset:"))
    if provider != "KIS" or datatype != "PRICE_BAR" or contract["adjusted"] is not True:
        raise CoordinationError("review_required")
    if (
        raw.get("provider"),
        raw.get("asset_id"),
        raw.get("provider_symbol"),
        raw.get("provider_exchange_code"),
        raw.get("start_date"),
        raw.get("end_date"),
        raw.get("adjusted"),
    ) != (
        "KIS",
        asset,
        contract["symbol"],
        contract["exchange"],
        contract["start"],
        contract["end"],
        True,
    ):
        raise CoordinationError("contract_conflict")
    if contract["interval"] != "1D" or contract["source"] != "KIS":
        raise CoordinationError("invalid_contract")
    start, end = (
        date.fromisoformat(contract["start"]),
        date.fromisoformat(contract["end"]),
    )
    bars_by_date = {}
    for page in raw["pages"]:
        if str(page["payload"].get("rt_cd", "0")) != "0":
            raise CoordinationError("review_required")
        for item in page["payload"].get("output2", []):
            bar = normalize_daily_price(item)
            if not start <= bar["date"] <= end:
                continue
            # Providers can overlap page boundaries, but contradictory duplicates are not safe.
            previous = bars_by_date.get(bar["date"])
            if previous and previous != bar:
                raise CoordinationError("contract_conflict")
            bars_by_date[bar["date"]] = bar
    expected = [date.fromisoformat(day) for day in plan["expectedDates"]]
    close_only = [date.fromisoformat(day) for day in plan["closeOnlyDates"]]
    if plan["policyVersion"] == "adjusted-calendar-v2":
        from pipeline.collection_plan import eligible_sessions

        evidence = plan["calendarEvidence"]
        if not (
            evidence["calendar"]["start"]
            <= contract["start"]
            <= contract["end"]
            <= evidence["calendar"]["end"]
        ):
            raise CoordinationError("contract_conflict")
        sessions, _ = eligible_sessions(
            evidence["calendar"],
            evidence["sha256"],
            datetime.fromisoformat(evidence["asOf"]),
        )
        required_sessions = {
            day for day in sessions if contract["start"] <= day <= contract["end"]
        }
        if set(plan["expectedDates"]) != required_sessions - set(
            plan["closeOnlyDates"]
        ):
            raise CoordinationError("contract_conflict")
    selected, _ = select_snapshot_bars(
        list(bars_by_date.values()), expected, start, end, close_only
    )
    for bar in selected:
        if any(
            not Decimal(str(bar[key])).is_finite()
            for key in ("open", "high", "low", "close")
        ):
            raise CoordinationError("review_required")
        if bar["volume"] is not None and not Decimal(str(bar["volume"])).is_finite():
            raise CoordinationError("review_required")
    return [
        {
            "date": bar["date"].isoformat(),
            **{
                key: str(bar[key]) if bar[key] is not None else None
                for key in ("open", "high", "low", "close", "volume")
            },
        }
        for bar in selected
    ]


def price_validator(allowed_root):
    def validate(cur, plan, content):
        if price_content(cur, plan, allowed_root) != content:
            raise CoordinationError("contract_conflict")
        cur.execute(
            "SELECT identity FROM pipeline_recovery_work WHERE work_key=%s",
            (plan["workKey"],),
        )
        identity = cur.fetchone()[0]
        if identity["provider"] != "KIS":
            raise CoordinationError("invalid_contract")
        asset_id = int(identity["target"].removeprefix("asset:"))
        # Serializes publications for the asset, including first publication (no pointer yet).
        cur.execute("SELECT id FROM asset WHERE id=%s FOR UPDATE", (asset_id,))
        if cur.fetchone() is None:
            raise CoordinationError("review_required")
        cur.execute(
            "SELECT id,content_hash FROM price_chart_snapshot WHERE asset_id=%s AND default_for_chart FOR UPDATE",
            (asset_id,),
        )
        current = cur.fetchall()
        expected = (
            []
            if plan["expectedSnapshotId"] is None
            else [(plan["expectedSnapshotId"], plan["expectedSnapshotHash"])]
        )
        if current != expected:
            raise CoordinationError("contract_conflict")
        cur.execute(
            """SELECT (bar_time AT TIME ZONE 'UTC')::date,display_policy FROM price_bar
          WHERE asset_id=%s AND source='KIS' AND "interval"='1D'
          AND bar_time >= %s::date AT TIME ZONE 'UTC' AND bar_time < (%s::date+1) AT TIME ZONE 'UTC'""",
            (asset_id, identity["start"], identity["end"]),
        )
        policies = cur.fetchall()
        required = {str(day) for day, policy in policies if policy == "FULL_OHLC"}
        close_only = {str(day) for day, policy in policies if policy == "CLOSE_ONLY"}
        if close_only != set(plan["closeOnlyDates"]) or not required.issubset(
            plan["expectedDates"]
        ):
            raise CoordinationError("review_required")
        if current:
            cur.execute(
                "SELECT trade_date,open,high,low,close,volume FROM price_chart_snapshot_bar WHERE snapshot_id=%s",
                (current[0][0],),
            )
            previous_rows = cur.fetchall()
            if not {str(row[0]) for row in previous_rows}.issubset(
                {row["date"] for row in content}
            ):
                raise CoordinationError(
                    "review_required"
                )  # Never replace full history with a daily slice.
            if plan["policyVersion"] == "adjusted-calendar-v2":
                by_date = {row["date"]: row for row in content}
                for old in previous_rows:
                    candidate = by_date[str(old[0])]
                    numbers = tuple(
                        Decimal(candidate[k]) if candidate[k] is not None else None
                        for k in ("open", "high", "low", "close", "volume")
                    )
                    if old[1:] != numbers:
                        # Corporate-action/revision review, never silently switch history.
                        raise CoordinationError("review_required")
        if not plan.get("coverageEvidence"):
            raise CoordinationError("invalid_contract")
        # Explicit, bounded raw lifetime. No merging a stale adjusted generation.
        if (
            type(plan.get("maxRawAgeSeconds")) is not int
            or not 1 <= plan["maxRawAgeSeconds"] <= 21600
        ):
            raise CoordinationError("invalid_contract")
        cur.execute(
            """SELECT bool_and(collected_at<=clock_timestamp() AND
            collected_at>=clock_timestamp()-(%s*interval '1 second')) FROM source_object
            WHERE id IN (SELECT (s->>'id')::bigint FROM pipeline_recovery_raw_intent i,
            jsonb_array_elements(i.sources) s WHERE i.work_key=%s)""",
            (plan["maxRawAgeSeconds"], plan["workKey"]),
        )
        if cur.fetchone() != (True,):
            raise CoordinationError("review_required")
        return {
            "rows": len(content),
            "assetId": asset_id,
            "rawRevalidated": True,
            "previousSnapshot": plan["expectedSnapshotId"],
        }

    return validate


def publish_price(db, lease, plan_hash, allowed_root):
    with db.cursor() as cur:
        cur.execute(
            "SELECT content_hash FROM pipeline_recovery_candidate WHERE work_key=%s",
            (lease["workKey"],),
        )
        row = cur.fetchone()
    db.rollback()
    if not row:
        raise CoordinationError("review_required")
    digest = row[0]

    def write(cur):
        cur.execute(
            """SELECT c.plan,c.plan_hash,c.content,c.content_hash,v.plan_hash,v.content_hash
          FROM pipeline_recovery_candidate c JOIN pipeline_recovery_verification v USING(work_key)
          WHERE work_key=%s FOR UPDATE OF c,v""",
            (lease["workKey"],),
        )
        record = cur.fetchone()
        if (
            not record
            or record[1] != plan_hash
            or record[4] != plan_hash
            or record[3] != digest
            or record[5] != digest
        ):
            raise CoordinationError("contract_conflict")
        plan, _, content = record[:3]
        if (
            canonical_hash(plan) != plan_hash
            or canonical_hash(content) != digest
            or plan["workKey"] != lease["workKey"]
        ):
            raise CoordinationError("contract_conflict")
        evidence = price_validator(allowed_root)(cur, plan, content)
        contract, sources = load_raw(cur, lease["workKey"], allowed_root)
        source, _, _, raw = sources[0]
        collected = datetime.fromisoformat(raw["collected_at"])
        # Real snapshot/candidate tables, INSERT-only. Existing source rows are not changed.
        ids, row_hashes = [], []
        for bar in content:
            bar_hash = canonical_hash(bar)
            cur.execute(
                """INSERT INTO price_adjustment_candidate(source_object_id,asset_id,source,"interval",trade_date,
              open,high,low,close,volume,currency,collected_at,content_hash)
              VALUES(%s,%s,'KIS','1D',%s,%s,%s,%s,%s,%s,%s,%s,%s)
              ON CONFLICT(source_object_id,asset_id,source,"interval",trade_date) DO NOTHING""",
                (
                    source["id"],
                    evidence["assetId"],
                    bar["date"],
                    bar["open"],
                    bar["high"],
                    bar["low"],
                    bar["close"],
                    bar["volume"],
                    contract["currency"],
                    collected,
                    bar_hash,
                ),
            )
            cur.execute(
                """SELECT id,content_hash,open,high,low,close,volume,currency FROM price_adjustment_candidate
              WHERE source_object_id=%s AND asset_id=%s AND source='KIS' AND "interval"='1D' AND trade_date=%s""",
                (source["id"], evidence["assetId"], bar["date"]),
            )
            candidate = cur.fetchone()
            numbers = tuple(
                Decimal(bar[k]) if bar[k] is not None else None
                for k in ("open", "high", "low", "close", "volume")
            )
            if (
                candidate[1] != bar_hash
                or candidate[2:7] != numbers
                or candidate[7] != contract["currency"]
            ):
                raise CoordinationError("contract_conflict")
            ids.append(candidate[0])
            row_hashes.append(bar_hash)
            lineage_key = f"{evidence['assetId']}:KIS:1D:{bar['date']}"
            cur.execute(
                """INSERT INTO record_lineage(source_object_id,target_table,target_record_id,target_record_key,
                transformer_name,transformer_version,output_hash,processed_at)
                VALUES(%s,'price_adjustment_candidate',%s,%s,'coordinated-price','1',%s,clock_timestamp())
                ON CONFLICT(source_object_id,target_table,target_record_key,transformer_name,transformer_version) DO NOTHING""",
                (source["id"], candidate[0], lineage_key, bar_hash),
            )
            cur.execute(
                """SELECT target_record_id,output_hash FROM record_lineage WHERE source_object_id=%s
                AND target_table='price_adjustment_candidate' AND target_record_key=%s
                AND transformer_name='coordinated-price' AND transformer_version='1'""",
                (source["id"], lineage_key),
            )
            if cur.fetchone() != (candidate[0], bar_hash):
                raise CoordinationError("contract_conflict")
        cur.execute(
            """INSERT INTO price_chart_snapshot(asset_id,source_object_id,source,currency,basis,volume_basis,
          coverage_start,coverage_end,collected_at,row_count,content_hash)
          VALUES(%s,%s,'KIS',%s,'KIS_PROVIDER_ADJUSTED','PROVIDER_REPORTED',%s,%s,%s,%s,%s) RETURNING id""",
            (
                evidence["assetId"],
                source["id"],
                contract["currency"],
                content[0]["date"],
                content[-1]["date"],
                collected,
                len(content),
                content_hash(row_hashes),
            ),
        )
        snapshot_id = cur.fetchone()[0]
        for candidate_id, bar in zip(ids, content, strict=True):
            cur.execute(
                """INSERT INTO price_chart_snapshot_bar(snapshot_id,trade_date,candidate_id,open,high,low,close,volume)
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    snapshot_id,
                    bar["date"],
                    candidate_id,
                    bar["open"],
                    bar["high"],
                    bar["low"],
                    bar["close"],
                    bar["volume"],
                ),
            )
        cur.execute(
            "UPDATE price_chart_snapshot SET default_for_chart=false WHERE asset_id=%s AND default_for_chart",
            (evidence["assetId"],),
        )
        cur.execute(
            "UPDATE price_chart_snapshot SET default_for_chart=true WHERE id=%s",
            (snapshot_id,),
        )
        result = {
            "snapshotId": snapshot_id,
            "rows": len(content),
            "previousSnapshot": plan["expectedSnapshotId"],
        }
        cur.execute(
            "UPDATE pipeline_recovery_candidate SET published_result=%s::jsonb WHERE work_key=%s",
            (json.dumps(result), lease["workKey"]),
        )
        return result

    return apply_phase(db, lease, "VERIFIED", digest, write)


def verify_published_price(cur, key, plan_hash):
    """Rebuild exact serving rows + lineage using a separate caller connection."""
    cur.execute(
        "SELECT plan,plan_hash,content,content_hash,published_result FROM pipeline_recovery_candidate WHERE work_key=%s",
        (key,),
    )
    record = cur.fetchone()
    if (
        not record
        or record[1] != plan_hash
        or canonical_hash(record[0]) != plan_hash
        or canonical_hash(record[2]) != record[3]
        or not record[4]
    ):
        raise CoordinationError("contract_conflict")
    _plan, _, content, digest, result = record
    cur.execute(
        "SELECT contract,sources FROM pipeline_recovery_raw_intent WHERE work_key=%s",
        (key,),
    )
    intent = cur.fetchone()
    if not intent or len(intent[1]) != 1:
        raise CoordinationError("contract_conflict")
    contract, sources = intent
    asset = int(contract["target"].removeprefix("asset:"))
    cur.execute(
        """SELECT b.trade_date,b.open,b.high,b.low,b.close,b.volume,c.content_hash,
        c.open,c.high,c.low,c.close,c.volume,l.output_hash,l.target_record_id,c.id,
        c.asset_id,c.source_object_id,c.source,c."interval",c.currency,c.trade_date
        FROM price_chart_snapshot_bar b JOIN price_adjustment_candidate c ON c.id=b.candidate_id
        JOIN price_chart_snapshot s ON s.id=b.snapshot_id
        JOIN record_lineage l ON l.source_object_id=s.source_object_id AND l.target_record_id=c.id
          AND l.target_table='price_adjustment_candidate' AND l.transformer_name='coordinated-price' AND l.transformer_version='1'
        WHERE b.snapshot_id=%s ORDER BY b.trade_date""",
        (result["snapshotId"],),
    )
    actual = cur.fetchall()
    if len(actual) != len(content):
        raise CoordinationError("review_required")
    for saved, expected in zip(actual, content, strict=True):
        numbers = tuple(
            Decimal(expected[k]) if expected[k] is not None else None
            for k in ("open", "high", "low", "close", "volume")
        )
        expected_hash = canonical_hash(expected)
        if (
            str(saved[0]) != expected["date"]
            or saved[1:6] != numbers
            or saved[7:12] != numbers
            or saved[6] != expected_hash
            or saved[12] != expected_hash
            or saved[13] != saved[14]
            or saved[15:20]
            != (asset, sources[0]["id"], "KIS", "1D", contract["currency"])
            or saved[20] != saved[0]
        ):
            raise CoordinationError("contract_conflict")
    cur.execute(
        """SELECT default_for_chart,row_count,content_hash,asset_id,source_object_id,source,currency,
        basis,volume_basis,coverage_start,coverage_end FROM price_chart_snapshot WHERE id=%s""",
        (result["snapshotId"],),
    )
    if cur.fetchone() != (
        True,
        len(content),
        content_hash([canonical_hash(r) for r in content]),
        asset,
        sources[0]["id"],
        "KIS",
        contract["currency"],
        "KIS_PROVIDER_ADJUSTED",
        "PROVIDER_REPORTED",
        date.fromisoformat(content[0]["date"]),
        date.fromisoformat(content[-1]["date"]),
    ):
        raise CoordinationError("review_required")
    return {"hash": digest, "rows": len(content), "result": result}

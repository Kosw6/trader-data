"""Replay already collected public examples. --fetch reads only missing-title publisher URLs; no DB/AWS."""
import argparse
import json
from pathlib import Path

from collectors.article_title import ArticleTitleClient
from pipeline.news_collection import digest, normalize
from pipeline.news_enrichment import enrich_observations
from pipeline.news_targets import select_targets
from workers.news_enrichment_backfill import ReceiptRepository


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--examples", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fetch", action="store_true")
    args = parser.parse_args()
    gkg = json.loads((args.examples / "gdelt-selected.json").read_text(encoding="utf-8"))
    rss = json.loads((args.examples / "rss-selected.json").read_text(encoding="utf-8"))
    target = dict(gkg[0]["requestedAssets"][0], organizationNames=["nvidia"])
    query = '("nvidia" OR "NVDA stock")'
    import hashlib
    target["queryKey"] = hashlib.sha256(query.encode()).hexdigest()[:16]
    rows = [dict(url=r["aggregatorUrl"], title=r["title"], publisher=r["publisher"],
                 providerReportedPublishedAt=r["aggregatorReportedPublishedAt"]) for r in rss]
    selected, _ = select_targets(rows, dict(contract="news-v2", provider="RSS", targets=[target], queries={target["queryKey"]: query}), query)
    observations = normalize(selected, "RSS")[1] + normalize(gkg, "GDELT")[1]
    args.output.mkdir(parents=True, exist_ok=False)
    repo = ReceiptRepository(args.output); checkpoint = {"exampleInputHash": digest(observations)}
    client = ArticleTitleClient() if args.fetch else None
    enriched = enrich_observations(observations, checkpoint, repo, client)
    with (args.output / "enriched-examples.json").open("x", encoding="utf-8") as file:
        json.dump(dict(provenance="LOCAL_REPLAY_OF_COLLECTED_EXAMPLES_NOT_OPERATIONAL_DB", observations=enriched), file, ensure_ascii=False, indent=2)
    report = dict(ec2Starts=0, dbWrites=0, publisherRequests=client.requests if client else 0,
                  summary=checkpoint["titleEnrichment"]["summary"],
                  examples=[dict(provider=r["metadata"]["provider"], title=r["metadata"]["title"],
                                 titleStatus=r["metadata"]["titleEnrichment"], relevance=r["metadata"]["assetRelevance"]) for r in enriched])
    with (args.output / "receipt.json").open("x", encoding="utf-8") as file:
        json.dump(report, file, ensure_ascii=False, indent=2)
    print(json.dumps({k: report[k] for k in ("ec2Starts", "dbWrites", "publisherRequests", "summary")}))


if __name__ == "__main__": main()

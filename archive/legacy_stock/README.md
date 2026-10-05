# 폐기한 stock 테이블용 도구

가격 런타임 원본은 `price_bar`로 통일했다. 이 폴더의 이관/벤치마크는 과거 기록이며
현재 DB에서 실행하지 않는다. 현재 복구는 `backfills/price_bar_invalid_repair.py`,
수집 적재는 기존 `workers/price_persistence.py`를 사용한다.

`stock_info`는 가격 테이블이 아닌 종목 메타데이터이므로 별도 계약으로 유지한다.

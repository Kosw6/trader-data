from datetime import date
from unittest.mock import MagicMock
import pytest
from workers import financial_mapping_worker as worker

@pytest.mark.parametrize("refresh",[False,True])
def test_queue_receipt_keeps_requested_generation_and_defers_automatic_refresh(monkeypatch,refresh):
 connection=MagicMock();cur=connection.cursor.return_value.__enter__.return_value
 cur.fetchone.side_effect=[(1440,7,date(2014,1,1),None,True,refresh,'GO_MANUAL668'),(1,)]
 monkeypatch.setattr(worker,'apply_company',lambda *a,**k:{'companyId':1440,'receiptId':99,'results':{'EXISTING':4}})
 result=worker.process_one(connection)
 assert result['receiptId']==99;connection.commit.assert_called_once()
 calls=cur.execute.call_args_list
 receipt=next(c for c in calls if 'SET queue_generation=' in c.args[0]);assert receipt.args[1]==(7,99)
 queue=next(c for c in calls if 'UPDATE finance_mapping667.queue' in c.args[0])
 assert queue.args[1][0]==('PENDING' if refresh else 'DONE')
 assert 'CASE WHEN refresh_requested THEN 1 ELSE 0 END' in queue.args[0]
 verify=calls[-1];assert verify.args[1]==(99,1440,7) and 'JOIN finance_mapping667.queue' not in verify.args[0]


def test_commit_failure_is_never_reported_success(monkeypatch):
 connection=MagicMock();cur=connection.cursor.return_value.__enter__.return_value
 cur.fetchone.return_value=(1440,7,date(2014,1,1),None,True,False,'GO_MANUAL668')
 monkeypatch.setattr(worker,'apply_company',lambda *a,**k:{'companyId':1440,'receiptId':99,'results':{'EXISTING':4}})
 connection.commit.side_effect=RuntimeError('COMMIT_UNKNOWN')
 with pytest.raises(RuntimeError,match='COMMIT_UNKNOWN'):worker.process_one(connection)
 connection.rollback.assert_called_once()

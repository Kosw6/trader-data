import importlib.util
import unittest
from pathlib import Path
path=Path(__file__).resolve().parents[2]/'trader-ai/scripts/sec_foreign_annual430_apply.py'
spec=importlib.util.spec_from_file_location('annual_writer430',path)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)

class FakeCursor:
    def __init__(self):self.queries=[]
    def execute(self,sql,params):self.queries.append((sql,params))
    def fetchone(self):return (123,)

class InsertOnlyScopeTests(unittest.TestCase):
    def test_removes_upsert_updates_for_all_allowed_tables(self):
        for table in ('source_object','filing','financial_fact','record_lineage'):
            fake=FakeCursor();guard=module.InsertOnlyCursor(fake)
            guard.execute(f'INSERT INTO {table} (id) VALUES (%s) ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id RETURNING id',(5,))
            sql,params=fake.queries[0]
            self.assertNotIn('UPDATE',sql);self.assertNotIn('ON CONFLICT',sql)
            self.assertEqual(params,(5,));self.assertEqual('RETURNING id' in sql,table!='record_lineage')

    def test_rejects_non_insert_and_other_tables(self):
        for sql in ('UPDATE company SET id=1','DELETE FROM financial_fact','INSERT INTO company (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET id=1'):
            with self.assertRaises(AssertionError):module.InsertOnlyCursor(FakeCursor()).execute(sql,())

    def test_rejects_unknown_sql_shape(self):
        with self.assertRaises(AssertionError):module.InsertOnlyCursor(FakeCursor()).execute('INSERT INTO financial_fact (id) VALUES (%s)',(1,))

if __name__=='__main__':unittest.main()

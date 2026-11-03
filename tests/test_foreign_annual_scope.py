import copy
import unittest
from datetime import datetime,timezone
from transformers.sec_transformer import TARGET_FORMS,transform_filings,transform_financial_facts

NOW=datetime(2026,10,21,tzinfo=timezone.utc)

class ForeignAnnualScopeTests(unittest.TestCase):
    def payload(self,form='20-F',taxonomy='us-gaap'):
        return {'cik':749098,'facts':{taxonomy:{'Assets':{'units':{'USD':[
            {'end':'2025-12-31','val':123,'accn':'0000749098-26-000001','form':form,'fy':2025,'fp':'FY','filed':'2026-03-01'}]}}}}}

    def scoped(self,payload):
        return transform_financial_facts(payload,'file:///preserved.json',NOW,
            allowed_forms={'20-F','40-F'},allowed_taxonomies={'us-gaap'})

    def test_default_still_excludes_foreign_forms(self):
        self.assertEqual(transform_financial_facts(self.payload(),'file:///x',NOW),[])
        self.assertEqual(TARGET_FORMS,{'10-K','10-Q','8-K'})

    def test_explicit_scope_preserves_raw_form_period_tag_and_value(self):
        for form in ('20-F','40-F'):
            row=self.scoped(self.payload(form))[0]
            self.assertEqual((row['form_type'],row['period_end'],row['period_start'],row['source_tag'],row['value']),
                             (form,'2025-12-31',None,'Assets','123'))

    def test_never_treats_six_k_as_quarterly(self):
        self.assertEqual(self.scoped(self.payload('6-K')),[])

    def test_taxonomy_filter_is_explicit(self):
        self.assertEqual(self.scoped(self.payload(taxonomy='ifrs-full')),[])

    def test_no_global_or_payload_mutation(self):
        payload=self.payload();before=copy.deepcopy(payload)
        self.scoped(payload)
        self.assertEqual(payload,before)
        self.assertEqual(transform_financial_facts(payload,'file:///x',NOW),[])

    def test_empty_form_scope_is_not_default_scope(self):
        self.assertEqual(transform_financial_facts(self.payload('10-K'),'file:///x',NOW,allowed_forms=set()),[])

    def test_source_hash_identity_does_not_relabel_facts(self):
        payload=self.payload('10-K')
        old=transform_financial_facts(payload,'file:///x',NOW)[0]
        scoped=transform_financial_facts(payload,'file:///x',NOW,allowed_forms={'10-K'},allowed_taxonomies={'us-gaap'})[0]
        self.assertEqual(old,scoped)

    def test_foreign_filing_requires_explicit_opt_in(self):
        submissions={'cik':749098}
        pages=[{'accessionNumber':['0000749098-26-000001'],'form':['40-F'],
                'filingDate':['2026-03-01'],'reportDate':['2025-12-31'],'primaryDocument':['annual.htm']}]
        self.assertEqual(transform_filings(submissions,pages,'file:///x',NOW),[])
        result=transform_filings(submissions,pages,'file:///x',NOW,allowed_forms={'20-F','40-F'})
        self.assertEqual(result[0]['form_type'],'40-F')
        self.assertEqual(result[0]['report_date'],'2025-12-31')

if __name__=='__main__':unittest.main()

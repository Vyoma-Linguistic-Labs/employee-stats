import argparse,ast,contextlib,csv,datetime,gc,hashlib,io,json,os,runpy,sys,tempfile,re
from pathlib import Path
from unittest.mock import patch
import requests
import unittest

def read_csv(path):
    with path.open(newline='', encoding='utf-8') as source:
        return list(csv.DictReader(source))


class ReportRegressionTests(unittest.TestCase):
    def test_cli_selection_exports_and_resume(self):
        source=(Path(__file__).resolve().parents[1] / 'allEmpStats.py').read_text()
        functions=[n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name in ('parse_arguments','find_employee_roster','load_employee_roster','merge_employee_roster')]
        ns=globals().copy()
        exec(compile(ast.Module(body=functions,type_ignores=[]),'<options>','exec'),ns)
        with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
            options=ns['parse_arguments'](['2026-09-01','2026-09-30','--entries-only'])
            assert options.start_seconds==1788201000
            assert options.end_seconds==1790792999
            assert options.period_label=='2026-09-01_to_2026-09-30'
            numeric=ns['parse_arguments'](['1788220800','1790812799'])
            assert numeric.start_seconds==1788220800 and numeric.end_seconds==1790812799
            leap=ns['parse_arguments'](['2024-02-29','2024-02-29'])
            assert leap.end_seconds-leap.start_seconds==86399
            for invalid in ([], ['2026-02-30','2026-03-01'], ['2026-10-01','2026-09-01'], ['1788220800000','1790812799000'], ['2026-09-01','2026-09-30','--typo']):
                try:ns['parse_arguments'](invalid)
                except SystemExit as exc:assert exc.code==2
                else:raise AssertionError('Invalid arguments accepted')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            try:ns['find_employee_roster'](options,root)
            except SystemExit as exc:assert '--current-only' in str(exc) and 'Historical employee roster missing' in str(exc)
            else:raise AssertionError('Missing roster not reported')
            current_options=argparse.Namespace(current_only=True,roster=None)
            assert ns['find_employee_roster'](current_options,root) is None
            explicit=argparse.Namespace(current_only=False,roster=root/'missing.json')
            try:ns['find_employee_roster'](explicit,root)
            except SystemExit as exc:assert 'Roster file not found' in str(exc)
            else:raise AssertionError('Missing explicit roster not reported')
            snapshot={'workspace_id':'3314662','date_range':'Apr25_Mar26','employees':[
                {'employee_code':'E001','name':'Former Person','clickup_ids':['111']},
                {'employee_code':'E002','name':'Existing Person','clickup_ids':['222']},
                {'employee_code':'E003','name':'Newly Resolved','clickup_ids':[]},
                {'employee_code':'E004','name':'Unknown Person','clickup_ids':[]}]}
            rosterpath=root/'allEmpStats-roster.json';rosterpath.write_text(json.dumps(snapshot))
            assert ns['find_employee_roster'](options,root)==rosterpath
            with contextlib.redirect_stdout(io.StringIO()):loaded=ns['load_employee_roster'](rosterpath,'3314662')
            team={'id':'3314662','members':[{'user':{'id':222,'username':'Existing Person - E002'}},{'user':{'id':333,'username':'Newly Resolved - E003'}}]}
            members,codes,missing=ns['merge_employee_roster'](loaded,team)
            assert set(members)=={111,222,333} and len(missing)==1 and codes[333]==['E003']
            script=root/'renamed_report.py';script.write_text(source)
            calls=[];phase=[1]
            class Response:
                ok=True;status_code=200
                def __init__(self,data):self.data=data
                def json(self):return self.data
            def fake_get(url,headers,params=None,timeout=None):
                assert headers['Authorization']=='fake-test-token'
                if url.endswith('/team'):return Response({'teams':[team]})
                if url.endswith('/space'):return Response({'spaces':[]})
                if '/task/' in url:
                    return Response({'time_spent':60000,'checklists':[],'date_created':None,'due_date':None,'list':{'name':'Test'},'folder':{'name':'Folder'},'start_date':None,'time_estimate':None,'assignees':[],'custom_fields':[{'name':name,'type':'drop_down','value':0,'type_config':{'options':[{'name':'Work'}]}} for name in ('Goal Type','Course')]})
                assert url.endswith('/time_entries')
                uid=params['assignee'];calls.append(uid)
                assert params['start_date']=='1788201000000' and params['end_date']=='1790792999999'
                if phase[0]==1 and len(calls)==2:raise KeyboardInterrupt()
                return Response({'data':[{'id':str(uid)+'entry','user':{'id':uid,'username':'Test'},'task':{'id':'t'+str(uid)},'duration':'60000','start':'1788201000000','end':'1788201060000'}]})
            cwd=Path.cwd();os.chdir(root)
            def run(*extra):
                with patch.object(requests,'get',fake_get),patch.dict(os.environ,{'CLICKUP_API_TOKEN':'fake-test-token'}),patch.object(sys,'argv',[str(script),'2026-09-01','2026-09-30',*extra]),contextlib.redirect_stdout(io.StringIO()):
                    try:runpy.run_path(str(script),run_name='__main__')
                    except SystemExit as exc:assert exc.code==0,exc
            try:
                try:run('--entries-only')
                except KeyboardInterrupt:pass
                phase[0]=2;gc.collect();run('--entries-only')
                assert set(calls)=={111,222,333} and calls.count(222)==1
                entries=root/'allEmployeeClickUp2026-09-01_to_2026-09-30_allEntries.csv'
                rows=read_csv(entries)
                assert len(rows)==3 and len({r['Time Entry ID'] for r in rows})==3
                coverage=read_csv(root/'allEmployeeClickUp2026-09-01_to_2026-09-30_employeeCoverage.csv')
                assert sum(r['Status']=='Unresolved ClickUp user ID' for r in coverage)==1
                assert all(r['Time Entries'] in ('','1') for r in coverage)
                gc.collect();before=len(calls);run('--entries-only');assert len(calls)==before
                gc.collect();run('--entries-only','--fresh');assert len(calls)==before+3
                gc.collect();run('--current-only')
                assert (root/'allEmployeeClickUp2026-09-01_to_2026-09-30.xlsx').exists()
                enriched=read_csv(root/'allEmployeeClickUp2026-09-01_to_2026-09-30.csv')
                assert len(enriched)==2
            finally:os.chdir(cwd)



if __name__ == "__main__":
    unittest.main()

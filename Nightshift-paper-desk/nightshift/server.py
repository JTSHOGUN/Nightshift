"""Same-origin web terminal + autonomous paper worker. No wallet/signing endpoints."""
import csv,io,os,sys,signal,time,json,threading,mimetypes,traceback
from pathlib import Path
from urllib.parse import urlsplit,parse_qs,unquote
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from store import ROOT,unpack,js
from feeds import GECKO,Missing,ADDR
from engine import Engine,MARK_SECONDS,SCAN_SECONDS
from papercheck import readiness,watchdog_ok

AGENTS=['Scout','Filter','Analyst','Risk','Executor','Logger']; WEB=ROOT/'public'
def dictrows(rows):return [dict(r) for r in rows]
class DeskHTTP(ThreadingHTTPServer):daemon_threads=True;allow_reuse_address=True
class Handler(BaseHTTPRequestHandler):
    server_version='Nightshift/1';sys_version=''
    def log_message(self,*args):pass
    @property
    def e(self):return self.server.engine
    def send(self,body,status=200,ctype='application/json; charset=utf-8',extras=None):
        if isinstance(body,(list,dict)):body=js(body).encode()
        if isinstance(body,str):body=body.encode()
        self.send_response(status);self.send_header('Content-Type',ctype);self.send_header('Content-Length',str(len(body)));self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff');self.send_header('Referrer-Policy','no-referrer')
        for k,v in (extras or {}).items():self.send_header(k,v)
        self.end_headers()
        try:self.wfile.write(body)
        except (BrokenPipeError,ConnectionResetError):pass
    def state(self,c,s):
        sid=s['id'];latest=c.execute('SELECT max(scan) FROM pairs WHERE sid=?',(sid,)).fetchone()[0] or 0
        latest=max(latest,c.execute("SELECT coalesce(max(scan),0) FROM events WHERE sid=? AND kind IN ('SCAN','CYCLE')",(sid,)).fetchone()[0])  # never relabel old pairs as current after an empty scan
        ps=dictrows(c.execute('SELECT pair,mint,symbol,name,created,seen,status,agent,reason,score,mcap,liq,vol,change,price,dex FROM pairs WHERE sid=? AND scan=? AND in_scope=1 ORDER BY score DESC,vol DESC',(sid,latest)))
        n=c.execute('SELECT count(*) FROM pairs WHERE sid=? AND scan=?',(sid,latest)).fetchone()[0];book=self.e.s.account(c,s);history=dictrows(c.execute('SELECT ts,value FROM (SELECT ts,value FROM equity WHERE sid=? ORDER BY ts DESC LIMIT 90) ORDER BY ts',(sid,)));recent=dictrows(c.execute("SELECT id,ts,agent,kind,symbol,reason,pair FROM events WHERE sid=? AND NOT(agent='Scout' AND kind IN ('KILL','HANDOFF')) ORDER BY id DESC LIMIT 7",(sid,)));count=c.execute('SELECT count(*) FROM events WHERE sid=?',(sid,)).fetchone()[0];agents=[]
        for a in AGENTS:
            row=c.execute('SELECT ts,reason,kind FROM events WHERE sid=? AND agent=? ORDER BY id DESC LIMIT 1',(sid,a)).fetchone();dec=c.execute('SELECT count(*) FROM events WHERE sid=? AND agent=?',(sid,a)).fetchone()[0];agents.append({'name':a,'working':bool(self.e.activity[a]),'count':dec,'last':dict(row) if row else None})
        return {'now':time.time(),'readiness':readiness(self.e,c,s,book),'session':s,'scan':latest,'scanning':self.e.scan_lock.locked(),'stats':{'screened':n,'in_range':len(ps),'kills':sum(p['status'] in ('killed','unverified','blocked') for p in ps),'qualified':sum(p['score'] is not None and p['score']>=70 for p in ps),'events':count},'pairs':ps,'book':book,'equity_history':history,'events':recent,'agents':agents,'feeds':dict(self.e.f.health),'scan_seconds':SCAN_SECONDS,'mark_seconds':MARK_SECONDS,'smart_allowlist':len(self.e.allow)}
    def do_GET(self):
        u=urlsplit(self.path);path=unquote(u.path);q={k:v[-1] for k,v in parse_qs(u.query).items()};store=self.e.s
        try:
            if not path.startswith('/api/'):
                f=(WEB/('index.html' if path=='/' else path.lstrip('/'))).resolve()
                if not f.is_relative_to(WEB) or not f.is_file():return self.send({'error':'Not found'},404)
                return self.send(f.read_bytes(),ctype=mimetypes.guess_type(str(f))[0] or 'application/octet-stream')
            with store.db() as c:
                s=store.session(c);sid=s['id']
                if path=='/api/state':return self.send(self.state(c,s))
                if path=='/api/readiness':return self.send(readiness(self.e,c,s))
                if path=='/api/health':return self.send({'paper_only':True,'watchdog_healthy':watchdog_ok(s,self.e.completed_risk_pulse,time.time()),'worker_heartbeat':s['heartbeat'],'session':s['mode'],'last_completed_scan':s['last_scan']})
                if path.startswith('/api/pair/'):
                    pair=path.rsplit('/',1)[-1];r=c.execute('SELECT * FROM pairs WHERE sid=? AND pair=?',(sid,pair)).fetchone()
                    if not r:return self.send({'error':'Pair not found in this session'},404)
                    p=dict(r);p['checks']=unpack(p['checks'],[]);p['factors']=unpack(p['factors'],[]);p['events']=dictrows(c.execute('SELECT id,ts,agent,kind,reason FROM events WHERE sid=? AND pair=? ORDER BY id DESC LIMIT 18',(sid,pair)));return self.send(p)
                if path in ('/api/events','/api/ledger.csv'):
                    clauses=['sid=?'];args=[sid]
                    for k in ['agent','kind']:
                        if q.get(k):clauses.append(k+'=?');args.append(q[k][:30])
                    if q.get('q'):clauses.append("instr(lower(coalesce(symbol,'')||' '||coalesce(reason,'')||' '||coalesce(mint,'')),lower(?))>0");args.append(q['q'][:150])
                    wh=' AND '.join(clauses)
                    if path.endswith('.csv'):
                        data=io.StringIO();w=csv.writer(data);keys=['id','sid','scan','ts','agent','kind','symbol','mint','pair','reason','detail'];w.writerow(keys)
                        for r in c.execute('SELECT * FROM events WHERE '+wh+' ORDER BY id',args):w.writerow([("'"+r[k] if isinstance(r[k],str) and r[k].lstrip().startswith(('=','+','-','@','\t','\r')) else r[k]) for k in keys]);
                        return self.send(data.getvalue(),ctype='text/csv; charset=utf-8',extras={'Content-Disposition':f'attachment; filename="nightshift-{sid}-audit.csv"'})
                    limit=max(1,min(100,int(q.get('limit',35))));offset=max(0,int(q.get('offset',0)));total=c.execute('SELECT count(*) FROM events WHERE '+wh,args).fetchone()[0];rows=dictrows(c.execute('SELECT id,ts,scan,agent,kind,symbol,mint,pair,reason FROM events WHERE '+wh+' ORDER BY id DESC LIMIT ? OFFSET ?',args+[limit,offset]));return self.send({'total':total,'rows':rows,'offset':offset,'limit':limit})
                if path=='/api/event':
                    r=c.execute('SELECT * FROM events WHERE id=? AND sid=?',(int(q.get('id',0)),sid)).fetchone()
                    if not r:return self.send({'error':'Event not found'},404)
                    d=dict(r);d['detail']=unpack(d['detail'],{});return self.send(d)
                if path=='/api/chart':
                    pair=q.get('pair','');span=q.get('range','1H')
                    if span not in ('1H','6H','24H') or not ADDR.fullmatch(pair) or not c.execute('SELECT 1 FROM pairs WHERE sid=? AND pair=?',(sid,pair)).fetchone():return self.send({'error':'Unknown pair or range'},400)
                    step,limit={'1H':(1,60),'6H':(5,72),'24H':(15,96)}[span];d=self.e.f.fetch('GeckoTerminal',GECKO+'/pools/'+pair+'/ohlcv/minute?aggregate='+str(step)+'&limit='+str(limit)+'&currency=usd&token=base',ttl=60);start=time.time()-{'1H':3600,'6H':21600,'24H':86400}[span];return self.send({'rows':sorted([r for r in d['data']['attributes']['ohlcv_list'] if len(r)>=6 and r[0]>=start-step*60]),'source':'GeckoTerminal','interval':step})
                if path=='/api/report':
                    f=Path(store.path).parent/('session-'+sid+'.md')
                    if not f.exists():return self.send({'error':'Report becomes available after session reconciliation'},409)
                    return self.send(f.read_bytes(),ctype='text/markdown',extras={'Content-Disposition':f'attachment; filename="nightshift-{sid}-report.md"'})
                return self.send({'error':'Unknown endpoint'},404)
        except (ValueError,OverflowError):return self.send({'error':'Invalid query'},400)
        except Missing as e:return self.send({'error':str(e)},503)
        except Exception as e:print('Request:',type(e).__name__,flush=True);return self.send({'error':'Request could not be completed'},500)
    def do_POST(self):
        if self.path!='/api/action':return self.send({'error':'Not found'},404)
        if self.headers.get('X-Desk-Action')!='paper-control' or not self.headers.get('Content-Type','').startswith('application/json'):return self.send({'error':'Same-origin action required'},403)
        try:
            size=int(self.headers.get('Content-Length','0'))
            if not 0<size<1024:return self.send({'error':'Invalid request size'},400)
            a=json.loads(self.rfile.read(size))['action'];store=self.e.s
            with store.db(immediate=True) as c:
                s=store.session(c);sid=s['id'];now=time.time()
                if a=='new' and s['mode']=='ended':store.new(c);msg='New overnight paper session opened'
                elif now>=s['end'] or s['mode'] in ('ended','closing'):return self.send({'error':'Session is closed to new work'},409)
                elif a=='scan' and s['mode']=='running':
                    if self.e.scan_lock.locked():return self.send({'error':'A scan is already in progress'},409)
                    c.execute('UPDATE session SET next_scan=? WHERE id=?',(now,sid));msg='Discovery scan queued'
                elif a=='pause' and s['mode']=='running':c.execute("UPDATE session SET mode='paused' WHERE id=?",(sid,));msg='Entries paused. Open positions remain monitored.'
                elif a=='resume' and s['mode']=='paused':c.execute("UPDATE session SET mode='running',next_scan=? WHERE id=?",(now,sid));msg='Session resumed; discovery queued'
                else:return self.send({'error':'Action not available in this state'},409)
                store.log(c,sid,s['scans'],'Logger','CONTROL',msg)
            self.e.wake.set();return self.send({'ok':True,'message':msg})
        except (ValueError,KeyError,TypeError):return self.send({'error':'Invalid action'},400)
        except Exception:return self.send({'error':'Action could not be committed'},500)

def main():
    # Exactly one worker per database, including after portable deployment.
    import fcntl
    e=Engine();lock=open(e.s.path+'.lock','a');
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise SystemExit('Another desk worker already owns this database')
    port=int(os.getenv('PORT','8080'));srv=DeskHTTP(('0.0.0.0',port),Handler);srv.engine=e;e.launch();print(f'Nightshift • paper-only desk on 0.0.0.0:{port}',flush=True)
    try:srv.serve_forever()
    finally:e.stop.set();srv.server_close()
if __name__=='__main__':main()

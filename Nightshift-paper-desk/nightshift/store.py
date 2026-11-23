"""Durable, append-only audit and paper account. All times are UTC epoch seconds."""
import os, json, sqlite3, time, uuid
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from contextlib import contextmanager

ROOT=Path(__file__).resolve().parent
EAT=ZoneInfo('Africa/Kampala')
def js(o): return json.dumps(o,ensure_ascii=False,separators=(',',':'),allow_nan=False)
def unpack(t,d=None): return json.loads(t) if t else d

def cutoff(now=None):
    t=datetime.fromtimestamp(now or time.time(), EAT); end=t.replace(hour=7,minute=0,second=0,microsecond=0)
    return (end if end>t else end+timedelta(days=1)).timestamp()

class Store:
    def __init__(self,path=None):
        self.path=str(path or os.getenv('DESK_DB') or ROOT/'runtime'/'desk.sqlite'); Path(self.path).parent.mkdir(parents=True,exist_ok=True)
        with self.db() as c:
            c.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS session(id TEXT PRIMARY KEY,start REAL,end REAL,mode TEXT,seed REAL,cash REAL,last_scan REAL,next_scan REAL,scans INTEGER DEFAULT 0,heartbeat REAL,trail REAL DEFAULT .06);
            CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,sid TEXT NOT NULL,scan INTEGER,ts REAL,agent TEXT,kind TEXT,pair TEXT,mint TEXT,symbol TEXT,reason TEXT,detail TEXT);
            CREATE INDEX IF NOT EXISTS ev_sid ON events(sid,id); CREATE INDEX IF NOT EXISTS ev_pair ON events(sid,pair,id);
            CREATE TRIGGER IF NOT EXISTS audit_no_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'Append-only ledger'); END;
            CREATE TRIGGER IF NOT EXISTS audit_no_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'Append-only ledger'); END;
            CREATE TABLE IF NOT EXISTS pairs(sid TEXT,pair TEXT,mint TEXT,symbol TEXT,name TEXT,created REAL,first REAL,seen REAL,scan INTEGER,in_scope INTEGER,status TEXT,agent TEXT,reason TEXT,score REAL,mcap REAL,liq REAL,vol REAL,change REAL,price REAL,dex TEXT,checks TEXT,factors TEXT,PRIMARY KEY(sid,pair));
            CREATE TABLE IF NOT EXISTS holders(mint TEXT,ts REAL,n INTEGER,source TEXT,PRIMARY KEY(mint,ts)); CREATE INDEX IF NOT EXISTS hx ON holders(mint,ts);
            CREATE TABLE IF NOT EXISTS history(mint TEXT PRIMARY KEY,checked REAL,rug INTEGER);
            CREATE TABLE IF NOT EXISTS trades(tx TEXT,mint TEXT,wallet TEXT,ts REAL,side TEXT,qty REAL,usd REAL,PRIMARY KEY(tx,mint,wallet,side)); CREATE INDEX IF NOT EXISTS trades_i ON trades(wallet,mint,ts);
            CREATE TABLE IF NOT EXISTS positions(id INTEGER PRIMARY KEY AUTOINCREMENT,sid TEXT,mint TEXT,pair TEXT,symbol TEXT,entered REAL,entry REAL,qty REAL,cost REAL,mark REAL,marked REAL,peak REAL,stop REAL,trail REAL,armed INTEGER DEFAULT 0,closed REAL,exit REAL,proceeds REAL,reason TEXT,entry_tx TEXT,exit_tx TEXT,liq REAL,UNIQUE(sid,mint));
            CREATE TABLE IF NOT EXISTS equity(sid TEXT,ts REAL,value REAL,PRIMARY KEY(sid,ts));
            ''')
            if 'pending_exit' not in [x[1] for x in c.execute('PRAGMA table_info(positions)')]:c.execute('ALTER TABLE positions ADD COLUMN pending_exit TEXT')
            if not c.execute('SELECT 1 FROM session LIMIT 1').fetchone(): self.new(c)
    @contextmanager
    def db(self,immediate=False):
        c=sqlite3.connect(self.path,timeout=15); c.row_factory=sqlite3.Row
        try:
            c.execute('PRAGMA busy_timeout=15000');
            if immediate:c.execute('BEGIN IMMEDIATE')
            yield c; c.commit()
        except BaseException:c.rollback();raise
        finally:c.close()
    def new(self,c):
        now=time.time();sid=uuid.uuid4().hex[:10];seed=10000.
        c.execute('INSERT INTO session(id,start,end,mode,seed,cash,last_scan,next_scan,heartbeat) VALUES(?,?,?,?,?,?,?,?,?)',(sid,now,cutoff(now),'running',seed,seed,0,now,now))
        c.execute('INSERT INTO equity VALUES(?,?,?)',(sid,now,seed));self.log(c,sid,0,'Logger','SESSION','Overnight paper session opened. No signing or broadcasting.',detail={'initial_cash_usd':seed,'cutoff':cutoff(now),'timezone':'Africa/Kampala'})
        return sid
    def session(self,c):return dict(c.execute('SELECT * FROM session ORDER BY start DESC LIMIT 1').fetchone())
    def log(self,c,sid,scan,agent,kind,reason,p=None,detail=None):
        p=p or {}; c.execute('INSERT INTO events(sid,scan,ts,agent,kind,pair,mint,symbol,reason,detail) VALUES(?,?,?,?,?,?,?,?,?,?)',(sid,scan,time.time(),agent,kind,p.get('pair'),p.get('mint'),p.get('symbol'),str(reason)[:1000],js(detail or {})))
    def event(self,*args,**kwargs):
        with self.db() as c:self.log(c,*args,**kwargs)
    def put(self,c,sid,scan,p,status,agent,reason,checks=None,score=None,factors=None):
        now=time.time();keys=['mint','symbol','name','created','mcap','liq','vol','change','price','dex']; vals=[p.get(k) for k in keys]
        cols=['sid','pair','first','seen','scan','in_scope','status','agent','reason','checks','score','factors']+keys
        q='INSERT INTO pairs('+','.join(cols)+') VALUES('+','.join('?' for _ in cols)+') ON CONFLICT(sid,pair) DO UPDATE SET '+','.join(k+'=excluded.'+k for k in cols if k not in ('first','sid','pair'))
        c.execute(q,[sid,p['pair'],now,now,scan,int(status!='out'),status,agent,reason,js(checks or []),score,js(factors or [])]+vals)
    def account(self,c,s):
        rows=[dict(x) for x in c.execute('SELECT * FROM positions WHERE sid=? ORDER BY entered DESC',(s['id'],))];live=[x for x in rows if not x['closed']];eq=s['cash']+sum(x['qty']*x['mark'] for x in live)
        return {'equity':eq,'cash':s['cash'],'pnl':eq-s['seed'],'realized':sum(x['proceeds']-x['cost'] for x in rows if x['closed']),'open':len(live),'exposure':sum(x['qty']*x['mark'] for x in live),'positions':rows,'pending':sum(bool(x['pending_exit']) for x in live),'stale':any(time.time()-x['marked']>100 for x in live)}

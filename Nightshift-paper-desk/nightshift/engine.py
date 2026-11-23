"""Six deterministic agents. Data moves forward only through typed, checked handoffs."""
import os,time,math,json,threading,traceback
from collections import defaultdict
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor,as_completed
from contextlib import contextmanager
from pathlib import Path
from feeds import Feeds,Missing,n,get,ADDR
from store import Store,js,unpack,ROOT
from papercheck import PULSE_LIMIT, watchdog_ok, recent_side, exit_estimate, accounting, forward_results

SCAN_SECONDS=180; MARK_SECONDS=20; FEE=.003; SLIP=.0025
REQUIRED={'Liquidity','Hourly volume','Hourly move','Mint authority','Freeze authority','LP protection','Top 10 wallets','Developer holding','Creator history','Launch bundles'}
def check(k,value,valid,rule,source,raw=None):return {'key':k,'value':str(value),'result':'unknown' if valid is None else 'pass' if valid else 'fail','rule':rule,'source':source,'at':time.time(),'raw':raw}
def numcheck(k,v,pred,rule,src,fmt,raw=None):return check(k,'Unavailable' if v is None else fmt(v),None if v is None else bool(pred(v)),rule,src,v if raw is None else raw)
def pct(v):return f'{v:,.2f}%'
def money(v):return f'${v:,.0f}'
def scope(p,now=None):
    t=now or time.time();age=(t-p['created'])/3600 if p.get('created') else None
    return [numcheck('Pair age',age,lambda x:.5<=x<=72,'30 min–72 hours','DexScreener',lambda v:f'{v:.2f} h'),numcheck('Market cap',p.get('mcap'),lambda x:300000<=x<=20000000,'$300k–$20M; no FDV substitution','DexScreener',money)]
def market(p):return [numcheck('Liquidity',p.get('liq'),lambda v:v>=40000,'At least $40,000','DexScreener',money),numcheck('Hourly volume',p.get('vol'),lambda v:v>=50000,'At least $50,000','DexScreener',money),numcheck('Hourly move',p.get('change'),lambda v:-100<=v<80,'Below +80% in 1h','DexScreener',pct)]
def failures(checks):return [x for x in checks if x['result']!='pass']
def explain(ch):return '; '.join(x['key']+': '+(x['value']+' · '+x['rule'] if x['result']=='fail' else x['value']) for x in failures(ch)) or 'All checks passed'
@dataclass(frozen=True)
class ScoutTicket: p:dict; sid:str; scan:int; at:float; checks:tuple
@dataclass(frozen=True)
class FilterTicket: scout:ScoutTicket; checks:tuple; holders:int
@dataclass(frozen=True)
class ScoreTicket: filtered:FilterTicket; score:float; factors:tuple
@dataclass(frozen=True)
class Permit: scored:ScoreTicket; budget:float; fill:float; stop:float; qty:float; quote:dict; trail:float; liq:float; issued:float; sell_quote:dict; immediate_net:float

def complete_filter(ticket):return isinstance(ticket,FilterTicket) and {c['key'] for c in ticket.checks}==REQUIRED and not failures(ticket.checks) and not failures(ticket.scout.checks)
def weights(momentum,buyers,buys,sells,growth,ratio,smart):
    # No imputation: all six source measurements are required.
    vals=[momentum,buyers,buys,sells,growth,ratio,smart]
    if any(n(x) is None for x in vals) or min(buyers,buys,sells,ratio,smart)<0 or buys+sells==0:raise Missing('Incomplete score inputs')
    clip=lambda x:max(0,min(1,x));bs=buys/sells if sells else None
    rows=[('Momentum',20,clip(momentum/40),pct(momentum)+' / 1h','DexScreener'),('Unique buyers',20,clip(buyers/300),str(int(buyers))+' / 1h','GeckoTerminal'),('Buy / sell ratio',15,1 if bs is None else clip((bs-.75)/1.25),f'{int(buys):,} / {int(sells):,}','DexScreener'),('Holder growth',20,clip(growth/10),pct(growth)+' / ~15m','Solana RPC · owner snapshots'),('Liquidity / cap',15,clip(ratio/.2),pct(100*ratio),'DexScreener'),('Smart wallets',10,clip(smart/3),str(int(smart))+' observed matches','GeckoTerminal · matched swap signers')]
    factors=[{'key':a,'max':b,'points':round(b*c,2),'value':d,'source':e} for a,b,c,d,e in rows]
    return round(sum(x['points'] for x in factors),2),factors

def plan_budget(equity,cash,price,liq):
    if any(n(x) is None or x<=0 for x in [cash,equity,price,liq]):raise Missing('No valid paper buying power or quote')
    budget=math.floor(min(cash,equity*.08)*100)/100
    if budget<1:raise Missing('Insufficient paper buying power')
    # All-in budget includes fees. Impact uses the approximated quote-side reserve.
    fill=price*(1+SLIP+2*budget/liq);qty=(budget/(1+FEE))/fill
    return budget,fill,fill*.94,qty

def update_stop(pos,price):
    peak=max(pos['peak'],price);armed=bool(pos['armed']) or peak>=pos['entry']*1.12
    return peak,armed,max(pos['stop'],peak*(1-pos['trail']) if armed else pos['stop'])

class Engine:
    def __init__(self,store=None,feeds=None):
        self.s=store or Store();self.f=feeds or Feeds();self.stop=threading.Event();self.wake=threading.Event();self.scan_lock=threading.Lock();self.activity=defaultdict(int);self.alock=threading.Lock();self.last_error=None;self.period=SCAN_SECONDS;self.completed_risk_pulse=0.0
        self.allow={k for k in os.getenv('SMART_WALLETS','').split(',') if ADDR.fullmatch(k)}
    @contextmanager
    def doing(self,name):
        with self.alock:self.activity[name]+=1
        try:yield
        finally:
            with self.alock:self.activity[name]-=1
    def launch(self):
        # A crashed mid-scan ticket is never resumed or converted into an approval.
        with self.s.db() as c:
            ss=self.s.session(c)
            self.s.log(c,ss['id'],ss['scans'],'Logger','WORKER','Paper worker online; existing ledger retained',detail={'previous_heartbeat':ss['heartbeat'],'gap_seconds':round(max(0,time.time()-ss['heartbeat']),1)})
            for row in c.execute("SELECT * FROM pairs WHERE sid=? AND status IN ('screening','cleared','qualified')",(ss['id'],)).fetchall():
                p=dict(row);self.s.log(c,ss['id'],p['scan'],'Logger','KILL','Interrupted handoff; rescan required',p);c.execute("UPDATE pairs SET status='unverified',agent='Logger',reason='Interrupted handoff; rescan required' WHERE sid=? AND pair=?",(ss['id'],p['pair']))
        threading.Thread(target=self.worker,daemon=True,name='scout-clock').start();threading.Thread(target=self.monitor,daemon=True,name='risk-clock').start()
    def worker(self):
        while not self.stop.is_set():
            try:
                with self.s.db() as c:s=self.s.session(c)
                if s['mode']=='running' and time.time()<s['end'] and time.time()>=s['next_scan']:self.scan()
            except Exception as e:
                self.last_error='Scan interrupted; see audit log';print('SCAN:',type(e).__name__,str(e)[:180],flush=True)
                with self.s.db() as c:
                    s=self.s.session(c);self.s.log(c,s['id'],s['scans'],'Scout','ERROR','Scan interrupted: '+type(e).__name__);c.execute('UPDATE session SET next_scan=? WHERE id=?',(time.time()+60,s['id']))
            self.wake.wait(2);self.wake.clear()
    def record(self,t,status,agent,reason,checks,factors=None,score=None,kind='KILL'):
        with self.s.db(immediate=True) as c:
            self.s.put(c,t.sid,t.scan,t.p,status,agent,reason,checks,score,factors);self.s.log(c,t.sid,t.scan,agent,kind,reason,t.p,{'checks':checks,'score':score,'factors':factors or []})
    def history(self,r,p):
        src='RugCheck · indexed creator launches';rule='No reported past rugs';creator=r.get('creator');hist=r.get('creatorTokens')
        alerts=[(str(x.get('name',''))+' '+str(x.get('description',''))).lower() for x in r.get('risks') or []]
        if r.get('rugged') is True or any(('creator' in x or 'deployer' in x) and 'rug' in x for x in alerts):return check('Creator history','Rug flag reported',False,rule,src)
        if not isinstance(hist,list) or len(hist)>=50:
            try:
                indexed=self.f.tracker_history(creator,p['mint']);bad=[m for m,v in indexed.items() if v];return check('Creator history','Indexed rug confirmed' if bad else f'{len(indexed)-1} prior launches clear',not bad,rule,'SolanaTracker · complete creator index',{'creator':creator,'indexed_mints':list(indexed),'rug_flags':bad})
            except Missing:return check('Creator history','History unavailable' if not isinstance(hist,list) else 'Creator list reached 50-row index limit',None,rule,src)
        if not ADDR.fullmatch(creator or ''):return check('Creator history','Unknown creator',None,rule,src)
        mints=[]
        for row in hist:
            m=row.get('mint') if isinstance(row,dict) else None
            if not m or not ADDR.fullmatch(m):return check('Creator history','Incomplete deployment list',None,rule,src)
            if row.get('rugged') is True:return check('Creator history','Rugged prior deployment',False,rule,src,m)
            if m!=p['mint']:mints.append(m)
        todo=[];isrug=False
        with self.s.db() as c:
            for m in set(mints):
                old=c.execute('SELECT rug,checked FROM history WHERE mint=?',(m,)).fetchone()
                if old and old['rug']:isrug=True;break
                if not old or time.time()-old['checked']>3600:todo.append(m)
        if isrug:return check('Creator history','Prior rug confirmed',False,rule,src)
        for m in todo[:3]:
            try:prev=self.f.report(m)
            except Missing:continue
            if prev.get('creator')!=creator or not isinstance(prev.get('rugged'),bool):continue
            with self.s.db() as c:c.execute('INSERT OR REPLACE INTO history VALUES(?,?,?)',(m,time.time(),int(prev['rugged'])))
            if prev['rugged']:return check('Creator history','Prior rug confirmed',False,rule,src,m)
        with self.s.db() as c:
            full=all(c.execute('SELECT 1 FROM history WHERE mint=? AND checked>? AND rug=0',(m,time.time()-3600)).fetchone() for m in set(mints))
        return check('Creator history',f'{len(set(mints))} indexed prior launches clear' if full else 'Prior launches not fully verified',True if full else None,rule,src,{'creator':creator,'prior_mints':mints})
    def security(self,t):
        p=t.p;ch=market(p);hc=None;r={}; owners=None;supply=0;slot=None
        # Expensive chain calls are only made after the cheap market kills pass.
        with ThreadPoolExecutor(max_workers=3) as ex:
            report=ex.submit(self.f.report,p['mint']);mint=ex.submit(self.f.mint,p['mint']);bundle=ex.submit(self.f.bundle,p)
            try:r=report.result()
            except Missing:pass
            try:
                info,program,slot=mint.result();supply=int(info['supply'])
                for key,label in [('mintAuthority','Mint authority'),('freezeAuthority','Freeze authority')]:
                    ch.append(check(label,('Revoked' if info[key] is None else 'Active') if key in info else 'Unavailable',info[key] is None if key in info else None,'Must be revoked','Solana RPC · confirmed',{'slot':slot,'authority':info.get(key)}))
                owners,supply,slot=self.f.owners(p['mint'],program,supply,slot);hc=len(owners)
                with self.s.db() as c:c.execute('INSERT INTO holders VALUES(?,?,?,?)',(p['mint'],time.time(),hc,'rpc-owner-census'))
            except (Missing,KeyError,ValueError,TypeError) as e:
                for label in ['Mint authority','Freeze authority']:
                    if label not in [x['key'] for x in ch]:ch.append(check(label,'Not verified',None,'Must be revoked','Solana RPC'))
            creator=r.get('creator');top=100*sum(sorted(owners.values(),reverse=True)[:10])/supply if owners is not None else None
            dev=100*owners.get(creator,0)/supply if owners is not None and ADDR.fullmatch(creator or '') else None
            ch.append(numcheck('Top 10 wallets',top,lambda x:x<=25,'At most 25% (vaults included)','Solana RPC · all owners',pct,{'slot':slot,'percent':top,'holders':hc}));ch.append(numcheck('Developer holding',dev,lambda x:x<=3,'At most 3%','RugCheck creator + RPC balances',pct,{'slot':slot,'percent':dev,'creator':creator}))
            pool=next((m for m in r.get('markets') or [] if m.get('pubkey')==p['pair'] and p['mint'] in [m.get('mintA'),m.get('mintB')]),{});locked=n(get(pool,'lp','lpLockedPct'));total_lp=n(get(pool,'lp','lpTotalSupply'));unlocked=n(get(pool,'lp','lpUnlocked'))
            valid_lp=locked is not None and 0<=locked<=100.0000001 and total_lp is not None and total_lp>0 and unlocked is not None and unlocked>=0
            ch.append(check('LP protection',pct(locked)+' protected' if valid_lp else 'Exact-pool LP evidence unavailable',(locked>=99.999999 and unlocked==0) if valid_lp else None,'100% burned or locked; zero unlocked LP','RugCheck · exact pool',{'pool':p['pair'],'lp_mint':pool.get('mintLP'),'locked_pct':locked,'unlocked':unlocked,'total_lp':total_lp}))
            ch.append(self.history(r,p))
            try:v,src=bundle.result();ch.append(numcheck('Launch bundles',v,lambda x:0<=x<=10,'At most 10% of initial supply',src,pct))
            except Missing as e:ch.append(check('Launch bundles',str(e),None,'At most 10% of initial supply','TrenchBot / optional SolanaTracker'))
        return ch,hc
    def observe(self,p):
        trades=self.f.trades(p)
        with self.s.db() as c:c.executemany('INSERT OR IGNORE INTO trades(tx,mint,wallet,ts,side,qty,usd) VALUES(?,?,?,?,?,?,?)',[(x['tx'],x['mint'],x['wallet'],x['ts'],x['side'],x['qty'],x['usd']) for x in trades])
        return trades
    def smart_count(self,p,trades):
        if not trades or time.time()-trades[0]['ts']>120:raise Missing('Fresh swap sample unavailable for smart-wallet matching')
        buys={}
        for a in trades:
            if a['side']=='buy':buys[a['wallet']]=max(a['ts'],buys.get(a['wallet'],0))
        matches=set(buys)&self.allow
        # Use only closed, observed round trips in OTHER mints before the candidate buy.
        # This is a sampled evidence cohort, not a claim about a wallet's lifetime P&L.
        with self.s.db() as c:
            for wallet,cut in buys.items():
                if wallet in matches:continue
                rows=c.execute('SELECT mint,side,qty,usd FROM trades WHERE wallet=? AND mint!=? AND ts<? AND ts>? ORDER BY mint,ts,tx',(wallet,p['mint'],cut,cut-604800)); books={};wins=[]
                for x in rows:
                    m=x['mint'];b=books.setdefault(m,[0.,0.,0.]);q=x['qty']
                    if x['side']=='buy':b[0]+=q;b[1]+=x['usd']*(1+FEE)
                    elif b[0]>0 and q<=b[0]*1.00001:
                        b[0]-=q;b[2]+=x['usd']*(1-FEE)
                        if b[0]<=max(1e-9,q*.00001):wins.append((m,b[2]-b[1]));books[m]=[0.,0.,0.]
                    else:books[m]=[0.,0.,0.]
                if len(wins)>=3 and len({m for m,v in wins})>=2 and sum(v for m,v in wins)>0 and sum(v>0 for m,v in wins)/len(wins)>=2/3:matches.add(wallet)
        return len(matches),{'matched_wallets':sorted(matches),'sample_swaps':len(trades),'sample_start':min(x['ts'] for x in trades),'allowlist_size':len(self.allow)}
    def analyze(self,ft):
        if not complete_filter(ft):raise Missing('Incomplete Filter handoff')
        t=ft.scout;p=t.p
        with self.s.db() as c:old=c.execute("SELECT * FROM holders WHERE mint=? AND source='rpc-owner-census' AND ts BETWEEN ? AND ? ORDER BY abs(ts-?) LIMIT 1",(p['mint'],time.time()-1200,time.time()-720,time.time()-900)).fetchone()
        if not old or old['n']<=0:raise Missing('Holder-growth baseline warming up (12–20 min required)')
        growth=(ft.holders/old['n']-1)*100;buyers=self.f.buyers(p);trades=self.observe(p);smart,meta=self.smart_count(p,trades);score,fs=weights(p['change'],buyers,p['buys'],p['sells'],growth,p['liq']/p['mcap'],smart);fs[-1]['evidence']=meta;fs[3]['evidence']={'previous':old['n'],'current':ft.holders,'previous_at':old['ts']}
        return ScoreTicket(ft,score,tuple(fs))
    def process_ticket(self,t):
        p=t.p;ch=market(p)
        try:
            with self.doing('Filter'):
                if not failures(ch):ch,hc=self.security(t)
                else:hc=None
                bad=failures(ch);all_checks=list(t.checks)+ch
                if bad:
                    status='killed' if any(v['result']=='fail' for v in bad) else 'unverified';self.record(t,status,'Filter',explain(ch),all_checks);return
                ft=FilterTicket(t,tuple(ch),hc)
                if not complete_filter(ft):raise Missing('Missing a mandatory Filter check')
                self.record(t,'cleared','Filter','All 10 hard checks passed → Analyst',all_checks,kind='HANDOFF')
            with self.doing('Analyst'):
                try:st=self.analyze(ft)
                except Missing as e:self.record(t,'unverified','Analyst',str(e),all_checks);return
                if st.score<70:self.record(t,'killed','Analyst',f'Score {st.score:g} below 70',all_checks,list(st.factors),st.score);return
                self.record(t,'qualified','Analyst',f'Score {st.score:g}/100 → Risk',all_checks,list(st.factors),st.score,kind='HANDOFF');return st
        except Exception as e:
            self.record(t,'unverified','Filter','No approval: '+(str(e)[:150] if isinstance(e,Missing) else 'incomplete source response ('+type(e).__name__+')'),list(t.checks)+ch)
    def permit(self,st):
        if not isinstance(st,ScoreTicket) or not complete_filter(st.filtered) or not 70<=st.score<=100 or len(st.factors)!=6:raise Missing('Invalid score handoff')
        t=st.filtered.scout;p=t.p
        if time.time()-t.at>180:raise Missing('Filter evidence expired before entry')
        snap=self.f.direct_pair(p);fresh=scope(snap)+market(snap)
        if failures(fresh):raise Missing('Pre-entry recheck: '+explain(fresh))
        tape=self.f.trades(p);now=time.time();q=recent_side(tape,p['mint'],'buy',now);sell=recent_side(tape,p['mint'],'sell',now)
        with self.s.db() as c:
            ss=self.s.session(c);book=self.s.account(c,ss)
            if not watchdog_ok(ss,self.completed_risk_pulse,time.time()):raise Missing('Risk monitor is not current; entries blocked')
            if not accounting(c,ss)['ok']:raise Missing('Cash does not reconcile to the paper ledger')
            if ss['id']!=t.sid or ss['mode']!='running' or time.time()>=ss['end']:raise Missing('Session not accepting entries')
            if book['stale'] or book['pending']:raise Missing('A portfolio mark is stale or a protective exit is pending')
            if c.execute('SELECT 1 FROM positions WHERE sid=? AND mint=?',(t.sid,p['mint'])).fetchone():raise Missing('One entry per mint per session; no re-entry')
        budget,fill,stop,qty=plan_budget(book['equity'],book['cash'],q['price'],snap['liq'])
        if stop>=q['price']:raise Missing('Modeled impact would immediately breach the initial stop')
        _,net=exit_estimate(qty,min(sell['price'],q['price']),snap['liq'],FEE,SLIP)
        if net<=budget*.94:raise Missing('Modeled immediate exit would already consume the 6% stop budget')
        return Permit(st,budget,fill,stop,qty,q,ss['trail'],snap['liq'],time.time(),sell,net)
    def execute(self,v):
        if not isinstance(v,Permit):raise Missing('Executor requires a Risk permit')
        t=v.scored.filtered.scout;p=t.p
        with self.s.db(immediate=True) as c:
            s=self.s.session(c);book=self.s.account(c,s);now=time.time()
            if not watchdog_ok(s,self.completed_risk_pulse,now) or not accounting(c,s)['ok']:raise Missing('Risk heartbeat or paper ledger failed the fill-time recheck')
            recent_side([v.quote],p['mint'],'buy',now);recent_side([v.sell_quote],p['mint'],'sell',now)
            if any(n(x) is None or x<=0 for x in (v.fill,v.budget,v.qty,v.stop,v.liq)) or not math.isclose(v.qty*v.fill*(1+FEE),v.budget,abs_tol=1e-7):raise Missing('Inconsistent paper fill amounts')
            _,net=exit_estimate(v.qty,min(v.quote['price'],v.sell_quote['price']),v.liq,FEE,SLIP)
            if net<=v.budget*.94:raise Missing('Round-trip cost exceeds the risk budget')
            if s['id']!=t.sid or s['mode']!='running' or now>=s['end'] or not 0<=now-v.issued<=8 or now-t.at>180 or not 0<=now-v.quote['ts']<=90 or book['stale'] or book['pending']:raise Missing('Risk permit expired or session paused')
            if not complete_filter(v.scored.filtered) or v.scored.score<70 or v.budget>min(s['cash'],book['equity']*.08)+1e-8 or abs(v.stop-v.fill*.94)>v.fill*1e-10:raise Missing('Risk bounds invalidated')
            if c.execute('SELECT 1 FROM positions WHERE sid=? AND mint=?',(s['id'],p['mint'])).fetchone():raise Missing('Duplicate mint blocked')
            journal=c.execute('SELECT status,score,scan FROM pairs WHERE sid=? AND pair=?',(t.sid,p['pair'])).fetchone()
            if not journal or journal['status']!='qualified' or journal['score']!=v.scored.score or journal['scan']!=t.scan:raise Missing('No current, persisted Analyst approval')
            # Risk approval with a concrete stop is journaled before the fill, atomically.
            self.s.log(c,t.sid,t.scan,'Risk','HANDOFF','Protective stop armed → Executor',p,{'budget':v.budget,'stop':v.stop,'entry':v.fill,'equity_at_entry':book['equity'],'trail_distance':v.trail,'arm_gain':.12,'quote_timestamp':v.quote['ts'],'buy_reference_swap':v.quote['tx'],'sell_reference_swap':v.sell_quote['tx'],'sell_quote_timestamp':v.sell_quote['ts'],'estimated_immediate_net_proceeds':net,'estimated_round_trip_cost':v.budget-net,'model':'Paper estimate, not executable depth'})
            c.execute('INSERT INTO positions(sid,mint,pair,symbol,entered,entry,qty,cost,mark,marked,peak,stop,trail,entry_tx,liq) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(t.sid,p['mint'],p['pair'],p['symbol'],now,v.fill,v.qty,v.budget,v.quote['price'],v.quote['ts'],v.fill,v.stop,v.trail,v.quote['tx'],v.liq));c.execute('UPDATE session SET cash=cash-? WHERE id=?',(v.budget,t.sid))
            self.s.put(c,t.sid,t.scan,p,'paper','Executor','Paper position opened',list(t.checks)+list(v.scored.filtered.checks),v.scored.score,list(v.scored.factors));self.s.log(c,t.sid,t.scan,'Executor','ENTRY',f'Paper buy · ${v.budget:,.2f}',p,{'reference_swap':v.quote['tx'],'fill':v.fill,'qty':v.qty,'stop':v.stop,'fee_rate':FEE});self.s.log(c,t.sid,t.scan,'Logger','BOOKED','Paper fill and stop committed',p)
    def scan(self):
        if not self.scan_lock.acquire(False):return
        try:
            with self.s.db() as c:
                s=self.s.session(c);sid=s['id'];i=c.execute("SELECT coalesce(max(scan),0)+1 FROM events WHERE sid=?",(sid,)).fetchone()[0]
                if s['mode']!='running' or time.time()>=s['end']:return
                self.s.log(c,sid,i,'Scout','SCAN',f'Discovery cycle {i:03d} started');retained=[x[0] for x in c.execute('SELECT DISTINCT mint FROM pairs WHERE sid=? AND created>? ORDER BY in_scope DESC,seen DESC LIMIT 110',(sid,time.time()-72*3600))]
            with self.doing('Scout'):pairs,errors=self.f.scout(retained)
            tickets=[]
            with self.s.db(immediate=True) as c:
                for e in errors:self.s.log(c,sid,i,'Scout','SOURCE',e)
                for m in getattr(self.f,'omissions',[]):self.s.log(c,sid,i,'Scout','KILL','No usable, matching Solana pair in discovery response',{'mint':m}, {'source':'DexScreener','result':'unknown'})
                for p in pairs:
                    sc=scope(p)
                    if failures(sc):
                        reason=explain(sc);self.s.put(c,sid,i,p,'out','Scout',reason,sc);self.s.log(c,sid,i,'Scout','KILL',reason,p,{'checks':sc});continue
                    self.s.put(c,sid,i,p,'screening','Scout','In range → Filter',sc);self.s.log(c,sid,i,'Scout','HANDOFF','Pair age and market cap in range → Filter',p,{'checks':sc});tickets.append(ScoutTicket(p,sid,i,p['observed'],tuple(sc)))
            passed=[]
            with ThreadPoolExecutor(max_workers=2) as pool:
                for f in as_completed([pool.submit(self.process_ticket,t) for t in tickets]):
                    st=f.result()
                    if st:passed.append(st)
            for st in sorted(passed,key=lambda s:-s.score):
                t=st.filtered.scout
                try:
                    with self.doing('Risk'):v=self.permit(st)
                    with self.doing('Executor'):self.execute(v)
                except Exception as e:self.record(t,'blocked','Risk',str(e) if isinstance(e,Missing) else 'Approval failed: '+type(e).__name__,list(t.checks)+list(st.filtered.checks),list(st.factors),st.score)
            with ThreadPoolExecutor(max_workers=3) as ex:list(ex.map(self.f.thumbnail,[t.p for t in tickets[:24]]))
            # Read-only observation for the empirical wallet cohort; never a handoff.
            with self.s.db() as c:has_open=c.execute('SELECT 1 FROM positions WHERE sid=? AND closed IS NULL LIMIT 1',(sid,)).fetchone()
            # Optional quote-coverage sampling never competes with monitoring an open book.
            # These diagnostics cannot create a ticket, touch cash, or count as a win.
            for t in sorted(tickets,key=lambda t:-(t.p['vol'] or 0))[:0 if has_open else 2]:
                try:
                    tape=self.observe(t.p);now=time.time();buy=recent_side(tape,t.p['mint'],'buy',now);sell=recent_side(tape,t.p['mint'],'sell',now)
                    self.s.event(sid,i,'Logger','TAPE_CHECK','Two-sided market prints observed; not a trade approval',t.p,{'ok':True,'buy_tx':buy['tx'],'buy_at':buy['ts'],'sell_tx':sell['tx'],'sell_at':sell['ts'],'paper_fills':0})
                except (Missing,ValueError,KeyError,TypeError) as e:
                    self.s.event(sid,i,'Logger','TAPE_CHECK','Two-sided tape not verified; no test fill',t.p,{'ok':False,'reason':str(e)[:160] if isinstance(e,Missing) else 'Incomplete swap data','paper_fills':0})
            with self.doing('Logger'),self.s.db(immediate=True) as c:
                now=time.time();c.execute('UPDATE session SET scans=scans+1,last_scan=?,next_scan=? WHERE id=?',(now,now+self.period,sid));self.s.log(c,sid,i,'Logger','CYCLE',f'Cycle {i:03d} archived · {len(pairs)} pairs screened, {len(tickets)} in range',detail={'screened':len(pairs),'in_range':len(tickets),'source_errors':errors});book=self.s.account(c,self.s.session(c));c.execute('INSERT OR REPLACE INTO equity VALUES(?,?,?)',(sid,now,book['equity']))
            print(f'Cycle {i}: {len(pairs)} screened, {len(tickets)} in range, {len(passed)} qualified.',flush=True)
        finally:self.scan_lock.release()
    def mark(self,s,pos,ending):
        p={'mint':pos['mint'],'pair':pos['pair'],'symbol':pos['symbol']};q=self.f.quote(p)
        if not 0<=time.time()-q['ts']<=90 or q['price']<=0:raise Missing('No fresh, positive mark')
        if q['ts']<pos['marked']:raise Missing('Out-of-order mark ignored')
        batch=self.f.trades(p)  # cached copy of the same observed feed
        with self.s.db(immediate=True) as c:
            row=c.execute('SELECT * FROM positions WHERE id=? AND closed IS NULL',(pos['id'],)).fetchone()
            if not row:return
            state=dict(row);why=row['pending_exit'];floor=max(row['entered'],row['marked'])
            # Scan the available intrapoll tape chronologically. Never claim execution at an
            # earlier tick: a trigger observed in the batch fills against the current quote.
            tape=sorted([x for x in batch if floor<x['ts']<=q['ts']],key=lambda x:x['ts'])
            if not tape and q['ts']>=row['entered']:tape=[q]
            for tick in tape:
                if why:break
                peak,armed,stop=update_stop(state,tick['price'])
                if armed and not state['armed']:self.s.log(c,s['id'],s['scans'],'Risk','TRAIL','Gain reached +12%; high-water trail armed',p,{'stop':stop,'peak':peak,'observed_swap':tick['tx']})
                state.update(peak=peak,armed=armed,stop=stop)
                if tick['price']<=stop:
                    why='Trailing stop' if armed else 'Protective stop';self.s.log(c,s['id'],s['scans'],'Risk','EXIT_ORDER',why,p,{'trigger':stop,'observed_mark':tick['price'],'reference_swap':tick['tx']})
            if ending and not why:
                why='Session cutoff';self.s.log(c,s['id'],s['scans'],'Risk','EXIT_ORDER',why,p,{'reference_swap':q['tx']})
            c.execute('UPDATE positions SET mark=?,marked=?,peak=?,armed=?,stop=?,pending_exit=? WHERE id=?',(q['price'],q['ts'],state['peak'],int(state['armed']),state['stop'],why,pos['id']))
        if not why:return
        # The trigger is durably latched before requesting execution liquidity.
        # A later rebound/outage cannot cancel an already-triggered protective exit.
        sell=recent_side(batch,p['mint'],'sell',time.time());snap=self.f.direct_pair(p);liq=snap['liq']
        if not liq or liq<=0 or max(time.time()-q['ts'],time.time()-sell['ts'])>90:raise Missing('No fresh executable liquidity estimate; exit pending')
        with self.s.db(immediate=True) as c:
            row=c.execute('SELECT * FROM positions WHERE id=? AND closed IS NULL AND pending_exit IS NOT NULL',(pos['id'],)).fetchone()
            if not row:return
            fill,proceeds=exit_estimate(row['qty'],min(q['price'],sell['price']),liq,FEE,SLIP)
            c.execute('UPDATE positions SET closed=?,exit=?,proceeds=?,reason=?,exit_tx=?,pending_exit=NULL WHERE id=?',(time.time(),fill,proceeds,why,sell['tx'],pos['id']));c.execute('UPDATE session SET cash=cash+? WHERE id=?',(proceeds,s['id']));self.s.log(c,s['id'],s['scans'],'Executor','EXIT',f'{why} · paper exit ${proceeds:,.2f}',p,{'fill':fill,'reference_swap':sell['tx'],'latest_mark_swap':q['tx'],'sell_observed_at':sell['ts'],'net_pnl':proceeds-row['cost']});self.s.log(c,s['id'],s['scans'],'Logger','BOOKED','Exit and paper proceeds committed',p)
    def begin_risk_sweep(self,c,s,now):
        # Monitored stops cannot be guaranteed while a host is suspended. Do not
        # invent history or carry exposed positions past an unobserved interval.
        gap=now-s['heartbeat']
        if gap>PULSE_LIMIT and s['mode']!='ended':
            self.completed_risk_pulse=0.0
            self.s.log(c,s['id'],s['scans'],'Risk','MONITOR_GAP',f'Monitor gap: {gap:.0f}s. Open exposure queued for protective exit.',detail={'gap_seconds':gap,'previous_heartbeat':s['heartbeat'],'no_backfilled_fills':True})
            for row in c.execute('SELECT * FROM positions WHERE sid=? AND closed IS NULL AND pending_exit IS NULL',(s['id'],)).fetchall():
                c.execute("UPDATE positions SET pending_exit='Risk monitor interruption' WHERE id=?",(row['id'],));self.s.log(c,s['id'],s['scans'],'Risk','EXIT_ORDER','Risk monitor interruption; awaiting a fresh sell-side reference',dict(row))
        c.execute('UPDATE session SET heartbeat=? WHERE id=?',(now,s['id']))
    def monitor(self):
        last_errors={}
        while not self.stop.is_set():
            try:
                with self.s.db(immediate=True) as c:
                    s=self.s.session(c);ending=time.time()>=s['end'] or s['mode'] in ('closing','ended');self.begin_risk_sweep(c,s,time.time());positions=[dict(x) for x in c.execute('SELECT * FROM positions WHERE sid=? AND closed IS NULL',(s['id'],))]
                    if ending and s['mode']!='ended':
                        mode='closing' if positions or self.scan_lock.locked() else 'ended';c.execute('UPDATE session SET mode=? WHERE id=?',(mode,s['id']))
                        if mode!=s['mode']:self.s.log(c,s['id'],s['scans'],'Logger','SESSION','Session ended; paper ledger sealed' if mode=='ended' else 'Cutoff reached; no new entries. Reconciling open work.')
                for row in positions:
                    try:
                        with self.doing('Risk'):self.mark(s,row,ending)
                    except Exception as e:
                        if time.time()-last_errors.get(row['id'],0)>120:
                            self.s.event(s['id'],s['scans'],'Risk','SOURCE',str(e) if isinstance(e,Missing) else 'Mark unavailable; no assumed fill',row);last_errors[row['id']]=time.time()
                if positions:
                    with self.s.db() as c:c.execute('INSERT INTO equity VALUES(?,?,?)',(s['id'],time.time(),self.s.account(c,self.s.session(c))['equity']))
                if ending:self.report(s)
                self.completed_risk_pulse=time.time()
            except Exception as e:
                self.completed_risk_pulse=0.0;print('Risk monitor:',type(e).__name__,flush=True)
            self.stop.wait(MARK_SECONDS)
    def report(self,s):
        from collections import Counter
        from datetime import datetime
        from store import EAT
        dest=Path(self.s.path).parent/('session-'+s['id']+'.md')
        with self.s.db() as c:
            ss=dict(c.execute('SELECT * FROM session WHERE id=?',(s['id'],)).fetchone())
            if ss['mode']!='ended' or dest.exists():return
            a=self.s.account(c,ss);performance=forward_results(c,ss,a);sid=s['id'];kills=c.execute("SELECT agent,count(*) AS n FROM events WHERE sid=? AND kind='KILL' GROUP BY agent",(sid,)).fetchall()
            unique=c.execute('SELECT count(*) FROM pairs WHERE sid=?',(sid,)).fetchone()[0];reasons=Counter();gaps=Counter();best={}
            for row in c.execute("SELECT detail FROM events WHERE sid=? AND kind='KILL' AND agent='Filter'",(sid,)):
                for x in unpack(row['detail'],{}).get('checks',[]):
                    if x['result']=='fail':reasons[x['key']]+=1
                    if x['result']=='unknown':gaps[x['key']]+=1
            for row in c.execute("SELECT ts,mint,symbol,detail FROM events WHERE sid=? AND agent='Analyst' AND kind='HANDOFF'",(sid,)):
                d=unpack(row['detail'],{});v=d.get('score')
                if v is not None and v>=70 and v>best.get(row['mint'],{}).get('score',-1):best[row['mint']]={'score':v,'symbol':row['symbol'],'ts':row['ts'],'factors':d.get('factors',[])}
        stamp=lambda t:datetime.fromtimestamp(t,EAT).strftime('%d %b %Y %H:%M:%S EAT')
        safe=lambda x: ''.join(c for c in str(x) if c.isalnum() or c in ' ._-$')[:40]
        lines=['# Nightshift · overnight paper report','',f"Session `{sid}` · {stamp(ss['start'])} to {stamp(ss['end'])}",'',f"{ss['scans']} completed cycles. {unique:,} distinct pairs observed. Bounded discovery, not a full-chain census.",'','## Paper account','',f"- Starting capital: ${ss['seed']:,.2f}",f"- Final equity: ${a['equity']:,.2f}",f"- Net paper P&L: ${a['pnl']:,.2f}",f"- Positions opened: {len(a['positions'])}",'','## Strongest fully qualified signals','']
        if not best:lines.append('No coin cleared every hard gate and the 70-point score threshold. No inferred scores.')
        for m,v in sorted(best.items(),key=lambda it:-it[1]['score'])[:15]:lines+= [f"- {safe(v['symbol'])}: **{v['score']:.2f}/100** at {stamp(v['ts'])}. Mint: `{m}`."]
        lines+=['','## Logged kills (decisions, not unique coins)','']+[f"- {x['agent']}: {x['n']:,}" for x in kills]
        lines+=['','### Failed hard gates','']+[f'- {k}: {v:,}' for k,v in reasons.most_common()]+['','### Unverified evidence','']+[f'- {k}: {v:,}' for k,v in gaps.most_common()]
        lines+=['','## Paper executions',''];
        if not a['positions']:lines.append('No paper fills.')
        for p in a['positions']:lines.append(f"- {safe(p['symbol'])}: spent ${p['cost']:,.2f}; exit {p['reason']}; net ${p['proceeds']-p['cost']:,.2f}.")
        lines+=['','Source snapshots, exact rejection reasons and score components are retained in the append-only SQLite journal and CSV export. Paper prices include modeled fees/slippage/impact; not live executions.']
        f=performance;lines+=['','## Forward paper scorecard','',f"- Closed round trips: {f['closed']}",f"- Realized net P&L: ${f['net_realized']:,.2f}",f"- Sampled maximum drawdown: {f['sampled_max_drawdown_pct'] or 0:.2f}% (recorded marks only)"]
        if f['closed']:lines+=[f"- Win rate: {f['win_rate_pct']:.1f}%",f"- Average net per closed trade: ${f['avg_net_per_close']:,.2f}",f"- Modeled trading fees on closes: ${f['modeled_fees_on_closes']:,.2f} (already included)"]
        else:lines.append('- Win rate and average return: not measured; no closed trades.')
        lines.append('Paper results do not establish future profitability. No automatic promotion to live execution.')
        tmp=dest.with_suffix('.tmp');tmp.write_text('\n'.join(lines)+'\n');tmp.replace(dest)

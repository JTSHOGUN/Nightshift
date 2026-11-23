"""Synthetic, isolated tests. Never use the live session ledger or send network requests."""
import sys, time, sqlite3, tempfile, unittest, base64
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
from datetime import datetime
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import Engine, ScoutTicket, FilterTicket, ScoreTicket, Permit, scope, market, failures, weights, plan_budget, update_stop, complete_filter
from feeds import Feeds, Missing, b58, DEX
from store import Store, cutoff, EAT

MINT=b58(b'm'*32);PAIR=b58(b'p'*32);OWNER=b58(b'o'*32);PRIOR=b58(b'z'*32)
def pair():
    return dict(mint=MINT,pair=PAIR,symbol='TEST',name='Isolated test only',created=time.time()-7200,mcap=1_000_000,liq=200_000,vol=100_000,change=40,price=1.0,buys=400,sells=100,dex='test',observed=time.time())

class IsolatedFeed(Feeds):
    def __init__(self):
        super().__init__();self.p=pair();self.initial_bundle=3;self.mint_data={'mintAuthority':None,'freezeAuthority':None,'supply':'1000000'};self.r={'mint':MINT,'creator':OWNER,'creatorTokens':[],'rugged':False,'markets':[{'pubkey':PAIR,'mintA':MINT,'mintB':PRIOR,'lp':{'lpLockedPct':100,'lpTotalSupply':200000,'lpUnlocked':0}}]};self.mark=1.;self.history_flag=False;self.quote_age=0.
    def scout(self,retained=()):self.p['observed']=time.time();return [dict(self.p)],[]
    def mint(self,mint):return self.mint_data,'test-program',100
    def report(self,mint):return self.r if mint==MINT else {'mint':mint,'creator':OWNER,'rugged':self.history_flag}
    def owners(self,*args):return {b58(i.to_bytes(32,'big')):10000 for i in range(100)},1000000,101
    def tracker_history(self,*args):raise Missing('No indexed history')
    def bundle(self,p):
        if self.initial_bundle is None:raise Missing('No launch evidence')
        return self.initial_bundle,'Test launch index'
    def trades(self,p):
        now=time.time()-self.quote_age
        return [dict(tx='test-'+side,wallet=OWNER,mint=MINT,qty=42,usd=42,price=self.mark,side=side,ts=now) for side in ('buy','sell')]
    def buyers(self,p):return 350
    def quote(self,p):
        if self.quote_age>90:raise Missing('Stale reference swap')
        return self.trades(p)[0]
    def direct_pair(self,p):return self.p
    def thumbnail(self,p):pass

class DeskTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.store=Store(Path(self.tmp.name)/'test.sqlite');self.f=IsolatedFeed();self.e=Engine(self.store,self.f);self.e.completed_risk_pulse=time.time()
        with self.store.db() as c:self.sid=self.store.session(c)['id']
    def tearDown(self):self.tmp.cleanup()
    def state(self):
        with self.store.db() as c:s=self.store.session(c);return s,self.store.account(c,s)
    def history(self):
        with self.store.db() as c:c.execute('INSERT INTO holders VALUES(?,?,?,?)',(MINT,time.time()-900,80,'rpc-owner-census'))
    def seed_entry(self):
        self.history();self.e.scan();s,b=self.state();self.assertEqual(b['open'],1);return s,b['positions'][0]
    def last_pair(self):
        with self.store.db() as c:return dict(c.execute('SELECT * FROM pairs WHERE sid=? AND pair=?',(self.sid,PAIR)).fetchone())
    def ticket(self):return ScoutTicket(self.f.p,self.sid,1,time.time(),tuple(scope(self.f.p)))
    def test_scout_edges_and_no_fdv_substitution(self):
        now=time.time()
        for age,cap,passes in [(.5,300000,True),(72,20000000,True),(.499,300000,False),(72.01,300000,False),(1,299999,False),(1,20000001,False),(1,None,False)]:
            p={**self.f.p,'created':now-age*3600,'mcap':cap,'fdv':1000000};self.assertEqual(not failures(scope(p,now)),passes)
    def test_market_boundaries(self):
        p={**self.f.p,'liq':40000,'vol':50000,'change':79.999};self.assertFalse(failures(market(p)))
        for k,value in [('liq',39999),('vol',49999),('change',80),('vol',None)]:self.assertTrue(failures(market({**p,k:value})))
    def test_unknown_authority_is_not_revoked(self):
        del self.f.mint_data['mintAuthority'];self.e.scan();p=self.last_pair();self.assertEqual(p['status'],'unverified');self.assertIn('Mint authority',p['reason']);self.assertIsNone(p['score'])
    def test_nonrevoked_freeze_rejected(self):
        self.f.mint_data['freezeAuthority']=OWNER;self.e.scan();self.assertEqual(self.last_pair()['status'],'killed')
    def test_lp_must_match_pool_and_be_fully_protected(self):
        self.f.r['markets'][0]['pubkey']=PRIOR;ch,_=self.e.security(self.ticket());self.assertEqual(next(c for c in ch if c['key']=='LP protection')['result'],'unknown');self.f.r['markets'][0]['pubkey']=PAIR;self.f.r['markets'][0]['lp']['lpUnlocked']=1
        ch,_=self.e.security(self.ticket());self.assertEqual(next(c for c in ch if c['key']=='LP protection')['result'],'fail')
    def test_missing_or_oversized_bundles_never_score(self):
        for val,status in [(None,'unverified'),(10.01,'killed')]:
            self.f.initial_bundle=val;self.e.scan();p=self.last_pair();self.assertEqual(p['status'],status);self.assertIsNone(p['score']);self.assertEqual(self.state()[1]['open'],0)
        self.f.initial_bundle=10;self.assertFalse(failures(self.e.security(self.ticket())[0]))
    def test_top10_and_dev_are_separate_gates(self):
        with patch.object(self.f,'owners',return_value=({OWNER:31000,PRIOR:969000},1000000,103)):
            ch,_=self.e.security(self.ticket());self.assertEqual({c['key'] for c in failures(ch)},{'Developer holding','Top 10 wallets'})
    def test_history_missing_and_rugged_block(self):
        self.f.r['creatorTokens']=None;self.assertEqual(self.e.history(self.f.r,self.f.p)['result'],'unknown');self.f.r['creatorTokens']=[{'mint':PRIOR}];self.f.history_flag=True;self.assertEqual(self.e.history(self.f.r,self.f.p)['result'],'fail')
    def test_incomplete_creator_index_blocks(self):
        self.f.r['creatorTokens']=[{'mint':PRIOR}]*50;self.assertEqual(self.e.history(self.f.r,self.f.p)['result'],'unknown')
    def test_baseline_warming_is_analyst_kill(self):
        self.e.scan();p=self.last_pair();self.assertEqual((p['agent'],p['status'],p['score']),('Analyst','unverified',None));self.assertEqual(self.state()[1]['open'],0)
    def test_scoring_bounds_missing_and_threshold(self):
        self.assertEqual(weights(100,1000,300,0,20,.5,10)[0],100);self.assertEqual(weights(-50,0,0,100,-5,0,0)[0],0)
        with self.assertRaises(Missing):weights(10,None,10,10,10,.2,1)
        self.history();self.f.p.update(change=0,buys=2,sells=100);self.e.scan();p=self.last_pair();self.assertEqual(p['agent'],'Analyst');self.assertLess(p['score'],70);self.assertEqual(self.state()[1]['open'],0)
    def test_all_six_agents_and_preentry_stop(self):
        s,p=self.seed_entry();self.assertEqual(p['cost'],800);self.assertAlmostEqual(p['stop'],p['entry']*.94);self.assertAlmostEqual(s['cash'],9200);self.assertAlmostEqual(p['qty']*p['entry']*1.003,800)
        with self.store.db() as c:
            e=list(c.execute('SELECT id,agent,kind FROM events WHERE sid=? ORDER BY id',(self.sid,)));self.assertEqual({x['agent'] for x in e},{'Scout','Filter','Analyst','Risk','Executor','Logger'});self.assertLess(next(x['id'] for x in e if x['agent']=='Risk'),next(x['id'] for x in e if x['kind']=='ENTRY'))
        with self.assertRaises(Missing):self.e.execute(self.ticket())
    def test_eight_percent_all_in_without_leverage(self):
        for equity,cash in [(10000,10000),(200000,10),(47.11,11)]:self.assertLessEqual(plan_budget(equity,cash,1.1,1e6)[0],min(equity*.08,cash))
    def test_no_reentry_or_double_debit(self):
        self.seed_entry();self.e.scan();s,b=self.state();self.assertEqual(len(b['positions']),1);self.assertEqual(s['cash'],9200);self.assertEqual(self.last_pair()['status'],'blocked')
    def test_pause_and_cutoff_prevent_entry(self):
        self.history();st=self.e.process_ticket(self.ticket());self.assertIsInstance(st,ScoreTicket);permit=self.e.permit(st)
        with self.store.db() as c:c.execute("UPDATE session SET mode='paused'")
        with self.assertRaises(Missing):self.e.execute(permit)
        with self.store.db() as c:c.execute("UPDATE session SET mode='running',end=?",(time.time()-1,))
        with self.assertRaises(Missing):self.e.permit(st)
        self.assertEqual(self.state()[1]['open'],0)
    def test_stale_risk_permit_and_incomplete_filter_rejected(self):
        self.history();st=self.e.process_ticket(self.ticket());v=self.e.permit(st)
        with self.assertRaises(Missing):self.e.execute(replace(v,issued=time.time()-9))
        with self.assertRaises(Missing):self.e.permit(replace(st,score=69.99))
        with self.assertRaises(Missing):self.e.permit(replace(st,filtered=replace(st.filtered,checks=st.filtered.checks[:-1])))
        self.assertEqual(self.state()[1]['open'],0)
    def test_stop_fills_at_observed_gap_not_trigger(self):
        s,p=self.seed_entry();self.f.mark=p['entry']*.80;self.e.mark(s,p,False);s,b=self.state();q=b['positions'][0];self.assertEqual(b['open'],0);self.assertEqual(q['reason'],'Protective stop');self.assertLess(q['exit'],q['stop']);self.assertAlmostEqual(s['cash'],9200+q['proceeds'])
    def test_trailing_activation_and_ratchet(self):
        s,p=self.seed_entry();self.f.mark=p['entry']*1.119;self.e.mark(s,p,False);p=self.state()[1]['positions'][0];self.assertFalse(p['armed']);self.f.mark=p['entry']*1.12;self.e.mark(s,p,False);p=self.state()[1]['positions'][0];self.assertTrue(p['armed']);self.assertAlmostEqual(p['stop'],self.f.mark*.94);stop=p['stop'];self.f.mark=p['entry']*1.1;self.e.mark(s,p,False);self.assertEqual(self.state()[1]['positions'][0]['stop'],stop)
    def test_trigger_survives_outage_and_restart(self):
        s,p=self.seed_entry();self.f.mark=p['stop']*.98;self.f.p['liq']=None
        with self.assertRaises(Missing):self.e.mark(s,p,False)
        p=self.state()[1]['positions'][0];self.assertEqual(p['pending_exit'],'Protective stop');self.assertFalse(p['closed']);self.f.mark=p['entry']*1.02;self.f.p['liq']=200000;Engine(Store(self.store.path),self.f).mark(s,p,False);self.assertEqual(self.state()[1]['open'],0)
    def test_intrapoll_dip_is_not_erased_by_rebound(self):
        s,p=self.seed_entry();ts=time.time();q={'ts':ts,'price':p['entry']*1.01,'tx':'recent','mint':MINT,'side':'sell'}
        # Ensure the observed intrapoll trade is after the simulated entry.
        dip={'ts':(p['entered']+ts)/2,'price':p['entry']*.9,'tx':'dip'}
        with patch.object(self.f,'quote',return_value=q),patch.object(self.f,'trades',return_value=[q,dip]):self.e.mark(s,p,False)
        self.assertEqual(self.state()[1]['positions'][0]['reason'],'Protective stop')
    def test_cutoff_with_stale_quote_no_invented_fill(self):
        s,p=self.seed_entry();self.f.quote_age=200
        with self.assertRaises(Missing):self.e.mark(s,p,True)
        self.assertEqual(self.state()[1]['open'],1);self.f.quote_age=0.;self.e.mark(s,p,True);self.assertEqual(self.state()[1]['positions'][0]['reason'],'Session cutoff')
    def test_audit_is_append_only_and_report_uses_journal(self):
        s,p=self.seed_entry();self.e.mark(s,p,True)
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store.db() as c:c.execute('DELETE FROM events')
        with self.store.db() as c:c.execute("UPDATE session SET mode='ended'")
        self.e.report(s);r=(Path(self.tmp.name)/('session-'+self.sid+'.md')).read_text();self.assertIn('Strongest fully qualified signals',r);self.assertIn('90.00/100',r);self.assertIn('Session cutoff',r)
    def test_overnight_end_is_next_local_0700(self):
        for hour,nextday in [(2,False),(7,True),(22,True)]:
            t=datetime(2026,11,23,hour,0,tzinfo=EAT);end=datetime.fromtimestamp(cutoff(t.timestamp()),EAT);self.assertEqual((end.day,end.hour,end.minute),(24 if nextday else 23,7,0))
    def test_rpc_aggregates_owners_across_accounts(self):
        program='test';owner=b'x'*32
        def account(o,qty):return {'account':{'space':165,'owner':program,'data':[base64.b64encode(o+qty.to_bytes(8,'little')).decode(),'base64']}}
        d={'context':{'slot':102},'value':[account(owner,12),account(owner,13),account(b'y'*32,75)]};f=Feeds()
        with patch.object(f,'rpc',return_value=d):
            wallets,total,slot=f.owners(MINT,program,100,101);self.assertEqual(wallets[b58(owner)],25);self.assertEqual(len(wallets),2)
            with self.assertRaises(Missing):f.owners(MINT,program,101,101)
    def test_smart_cohort_no_lookahead(self):
        t=time.time()-100
        with self.store.db() as c:
            for i in range(3):
                m='other'+str(i);c.execute('INSERT INTO trades VALUES(?,?,?,?,?,?,?)',('buy'+str(i),m,OWNER,t+i*3,'buy',10,100));c.execute('INSERT INTO trades VALUES(?,?,?,?,?,?,?)',('sell'+str(i),m,OWNER,t+i*3+1,'sell',10,110))
        sample=self.f.trades(self.f.p);self.assertEqual(self.e.smart_count(self.f.p,sample)[0],1);sample[0]['ts']=t-1;self.assertEqual(self.e.smart_count(self.f.p,sample)[0],0)

if __name__=='__main__':unittest.main()

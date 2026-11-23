"""Readiness regressions run against temporary paper ledgers only."""
import time, sys, tempfile, unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from datetime import datetime, timezone
from email.utils import format_datetime
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from test_desk import IsolatedFeed, MINT, PAIR, PRIOR
from engine import Engine, ScoutTicket, scope
from store import Store
from feeds import Missing,retry_delay
from papercheck import recent_side,readiness,accounting,exit_estimate,watchdog_ok

class PaperReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.s=Store(Path(self.temp.name)/'paper.sqlite');self.f=IsolatedFeed();self.e=Engine(self.s,self.f)
        self.e.completed_risk_pulse=time.time()
        with self.s.db() as c:
            self.sid=self.s.session(c)['id'];c.execute('INSERT INTO holders VALUES(?,?,?,?)',(MINT,time.time()-900,80,'rpc-owner-census'))
    def score(self):
        p=self.f.p;return self.e.process_ticket(ScoutTicket(p,self.sid,1,time.time(),tuple(scope(p))))
    def state(self):
        with self.s.db() as c:
            s=self.s.session(c);return s,self.s.account(c,s),readiness(self.e,c,s)
    def entry(self):
        st=self.score();v=self.e.permit(st);self.e.execute(v);s,b,_=self.state();return s,b['positions'][0]
    def test_empty_book_is_not_a_performance_sample(self):
        s,b,r=self.state();f=r['performance'];self.assertTrue(r['runtime_ready']);self.assertFalse(r['live_execution']);self.assertEqual(f['closed'],0);self.assertEqual(f['net_realized'],0)
        for key in ('win_rate_pct','avg_net_per_close','profit_factor','return_on_closed_cost_pct'):self.assertIsNone(f[key])
    def test_risk_monitor_fresh_at_both_handoffs(self):
        st=self.score();v=self.e.permit(st);self.e.completed_risk_pulse=time.time()-61
        with self.assertRaisesRegex(Missing,'heartbeat'):self.e.execute(v)
        with self.assertRaisesRegex(Missing,'monitor'):self.e.permit(st)
        self.assertEqual(self.state()[1]['open'],0);self.assertFalse(self.state()[2]['runtime_ready'])
    def test_new_worker_does_not_inherit_readiness(self):
        e=Engine(self.s,self.f);s,_,_=self.state();self.assertFalse(watchdog_ok(s,e.completed_risk_pulse,time.time()))
    def test_account_mismatch_prevents_authorization_and_fill(self):
        score=self.score();v=self.e.permit(score)
        with self.s.db() as c:c.execute('UPDATE session SET cash=cash+77')
        self.assertFalse(self.state()[2]['cash_reconciliation']['ok'])
        with self.assertRaisesRegex(Missing,'ledger'):self.e.execute(v)
        with self.assertRaisesRegex(Missing,'reconcile'):self.e.permit(score)
    def test_buy_only_pool_cannot_enter(self):
        st=self.score();buy=self.f.trades(self.f.p)[0]
        with patch.object(self.f,'trades',return_value=[buy]):
            with self.assertRaisesRegex(Missing,'sell-side'):self.e.permit(st)
        self.assertEqual(self.state()[1]['cash'],10000)
    def test_already_lossmaking_roundtrip_beyond_stop_fails(self):
        st=self.score();a,b=self.f.trades(self.f.p);b['price']=a['price']*.93
        with patch.object(self.f,'trades',return_value=[a,b]):
            with self.assertRaisesRegex(Missing,'6%'):self.e.permit(st)
    def test_sell_evidence_bound_to_mint_and_time(self):
        st=self.score();v=self.e.permit(st);now=time.time()
        for change in ({'mint':PRIOR},{'ts':now-91},{'ts':now+5},{'side':'buy'},{'price':float('nan')},{'tx':''}):
            with self.assertRaises(Missing):self.e.execute(replace(v,sell_quote={**v.sell_quote,**change}))
        self.assertEqual(self.state()[1]['open'],0)
    def test_no_sell_after_trigger_latches_exit_without_fill(self):
        s,pos=self.entry();self.f.mark=pos['entry']*.85;buy=self.f.trades(self.f.p)[0]
        with patch.object(self.f,'trades',return_value=[buy]):
            with self.assertRaisesRegex(Missing,'sell-side'):self.e.mark(s,pos,False)
        p=self.state()[1]['positions'][0];self.assertEqual(p['pending_exit'],'Protective stop');self.assertIsNone(p['closed']);self.assertFalse(self.state()[2]['runtime_ready'])
        self.e.mark(s,p,False);self.assertEqual(self.state()[1]['positions'][0]['exit_tx'],'test-sell')
    def test_monitor_gap_persists_exit_only_until_fresh_sell(self):
        s,p=self.entry();t=time.time();s['heartbeat']=t-160
        with self.s.db() as c:self.e.begin_risk_sweep(c,s,t)
        _,b,r=self.state();self.assertEqual(b['pending'],1);self.assertEqual(b['open'],1);self.assertFalse(r['runtime_ready']);self.assertEqual(r['recorded_monitor_gaps'],1);self.assertEqual(b['cash'],9200)
        self.e.mark(s,b['positions'][0],False);p=self.state()[1]['positions'][0];self.assertEqual(p['reason'],'Risk monitor interruption')
    def test_closes_only_net_returns_do_not_double_deduct_costs(self):
        s,p=self.entry();opened=self.state()[2]['performance'];self.assertEqual(opened['closed'],0);self.assertLess(opened['unrealized_mark_pnl'],0);self.f.mark=p['entry']*.8;self.e.mark(s,p,False)
        s,b,r=self.state();f=r['performance'];net=s['cash']-10000;self.assertLess(net,0);self.assertAlmostEqual(f['net_realized'],net);self.assertAlmostEqual(f['avg_net_per_close'],net);self.assertEqual(f['closed'],1);self.assertEqual(f['win_rate_pct'],0);self.assertEqual(f['profit_factor'],0);self.assertGreater(f['modeled_fees_on_closes'],0);self.assertGreater(f['sampled_max_drawdown_pct'],0);self.assertTrue(r['cash_reconciliation']['ok'])
    def test_diagnostic_probe_cannot_open_positions(self):
        self.f.initial_bundle=None;self.e.scan();s,b,r=self.state();self.assertEqual(s['cash'],10000);self.assertEqual(b['open'],0);self.assertEqual(r['performance']['closed'],0);self.assertEqual(r['unknown_gates']['Launch bundles'],1);self.assertTrue(r['tape_checks'][0]['ok']);self.assertEqual(r['tape_checks'][0]['paper_fills'],0)
    def test_retry_after_respected_in_seconds_and_http_date(self):
        now=1_700_000_000;self.assertEqual(retry_delay('1800',now),1800);self.assertEqual(retry_delay(format_datetime(datetime.fromtimestamp(now+900,timezone.utc),usegmt=True),now),900);self.assertGreaterEqual(retry_delay(None,now),20)

if __name__=='__main__':unittest.main()

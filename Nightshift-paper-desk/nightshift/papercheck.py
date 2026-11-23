"""Paper readiness and forward measurements. No network calls or order submission."""
import math, time, json
from collections import Counter
from feeds import Missing, n

PULSE_LIMIT=60.0
SWAP_LIMIT=90.0

def watchdog_ok(session, completed_pulse, now):
    """A new process must complete a risk sweep, not inherit an old green light."""
    return all(n(t) is not None and t>0 and 0<=now-t<=PULSE_LIMIT
               for t in (session.get('heartbeat'),completed_pulse))

def recent_side(tape, mint, side, now):
    """The caller supplies a tape fetched for one exact pool. Never synthesize a side."""
    eligible=[v for v in tape if v.get('mint')==mint and v.get('side')==side and
              isinstance(v.get('tx'),str) and v['tx'] and n(v.get('ts')) is not None and
              0<=now-v['ts']<=SWAP_LIMIT and n(v.get('price')) is not None and v['price']>0]
    if not eligible:raise Missing(f'No verified {side}-side swap in the last {int(SWAP_LIMIT)}s')
    return max(eligible,key=lambda v:v['ts'])

def exit_estimate(qty, price, liq, fee, slip):
    if any(n(x) is None or x<=0 for x in (qty,price,liq)) or not (0<=fee<1 and 0<=slip<1):
        raise Missing('Cannot estimate a positive paper liquidation value')
    gross=qty*price
    fill=price*(1-slip)/(1+2*gross/liq)
    proceeds=qty*fill*(1-fee)
    if not math.isfinite(proceeds) or proceeds<=0:raise Missing('Invalid estimated paper proceeds')
    return fill,proceeds

def accounting(c,s):
    rows=c.execute('''SELECT count(*) AS n,coalesce(sum(cost),0) AS spent,
                      coalesce(sum(CASE WHEN closed IS NOT NULL THEN proceeds ELSE 0 END),0) AS returned,
                      coalesce(sum(CASE WHEN cost IS NULL OR cost<=0 OR qty IS NULL OR qty<=0 OR
                       (closed IS NOT NULL AND (proceeds IS NULL OR proceeds<0)) THEN 1 ELSE 0 END),0) AS bad
                      FROM positions WHERE sid=?''',(s['id'],)).fetchone()
    expected=s['seed']-rows['spent']+rows['returned'];diff=s['cash']-expected
    return {'ok':rows['bad']==0 and s['cash']>=0 and math.isfinite(diff) and abs(diff)<.005,
            'expected_cash':expected,'difference':diff,'positions':rows['n']}

def forward_results(c,s,book):
    rows=[p for p in book['positions'] if p['closed'] is not None]
    nets=[p['proceeds']-p['cost'] for p in rows];count=len(rows);total=math.fsum(nets)
    wins=sum(v>0 for v in nets);gains=math.fsum(max(0,v) for v in nets);losses=math.fsum(-min(0,v) for v in nets)
    invested=math.fsum(p['cost'] for p in rows)
    # Fee dollars only. Slippage and impact are already in the entry/exit prices.
    fees=math.fsum(max(0.,p['cost']-p['entry']*p['qty'])+max(0.,p['exit']*p['qty']-p['proceeds']) for p in rows)
    peak=s['seed'];dd=0.;samples=0
    for val, in c.execute('SELECT value FROM equity WHERE sid=? ORDER BY ts',(s['id'],)):
        peak=max(peak,val);dd=max(dd,(peak-val)/peak if peak>0 else 0);samples+=1
    val=book['equity'];peak=max(peak,val);dd=max(dd,(peak-val)/peak if peak>0 else 0)
    return {'closed':count,'win_rate_pct':100*wins/count if count else None,'winners':wins,
            'net_realized':total,'avg_net_per_close':total/count if count else None,
            'return_on_closed_cost_pct':100*total/invested if invested>0 else None,
            'profit_factor':gains/losses if losses>0 else None,'losing_trades':sum(v<0 for v in nets),
            'modeled_fees_on_closes':fees,'sampled_max_drawdown_pct':100*dd if samples else None,
            'equity_samples':samples,'unrealized_mark_pnl':math.fsum(p['qty']*p['mark']-p['cost'] for p in book['positions'] if p['closed'] is None),
            'label':'No closed trades' if not count else 'Observed paper returns only'}

def readiness(engine,c,s,book=None):
    now=time.time();book=book if book is not None else engine.s.account(c,s);recon=accounting(c,s)
    pulse=engine.completed_risk_pulse; healthy=watchdog_ok(s,pulse,now)
    last=c.execute("SELECT scan,ts,detail FROM events WHERE sid=? AND agent='Logger' AND kind='CYCLE' ORDER BY id DESC LIMIT 1",(s['id'],)).fetchone()
    missing=Counter();failed=Counter();probes=[];qualified=0;screened=0;in_range=0
    if last:
        summary=json.loads(last['detail']);screened=summary.get('screened',0);in_range=summary.get('in_range',0)
        for row in c.execute("SELECT detail FROM events WHERE sid=? AND scan=? AND agent='Filter' AND kind='KILL'",(s['id'],last['scan'])):
            for check in json.loads(row['detail']).get('checks',[]):
                if check['result']=='fail':failed[check['key']]+=1
                elif check['result']=='unknown':missing[check['key']]+=1
        qualified=c.execute("SELECT count(*) FROM events WHERE sid=? AND scan=? AND agent='Analyst' AND kind='HANDOFF'",(s['id'],last['scan'])).fetchone()[0]
        for row in c.execute("SELECT symbol,pair,ts,detail FROM events WHERE sid=? AND scan=? AND kind='TAPE_CHECK' ORDER BY id DESC LIMIT 3",(s['id'],last['scan'])):
            probes.append({'symbol':row['symbol'],'pair':row['pair'],'at':row['ts'],**json.loads(row['detail'])})
    blockers=[]
    if s['mode']!='running' or now>=s['end']:blockers.append('Session not accepting entries')
    if not healthy:blockers.append('Risk monitor not current')
    if not recon['ok']:blockers.append('Cash ledger mismatch')
    if book['pending']:blockers.append('Protective exit pending')
    if book['stale']:blockers.append('Stale portfolio mark')
    if s['cash']<1:blockers.append('Insufficient paper cash')
    gaps=c.execute("SELECT count(*) FROM events WHERE sid=? AND kind='MONITOR_GAP'",(s['id'],)).fetchone()[0]
    return {'as_of':now,'session_id':s['id'],'cutoff':s['end'],'session_mode':s['mode'],'cash':s['cash'],'paper_equity':book['equity'],'mode':'paper_only','live_execution':False,'runtime_ready':not blockers,'runtime_blockers':blockers,
            'watchdog_healthy':healthy,'watchdog_limit_s':PULSE_LIMIT,'completed_pulse':pulse or None,
            'recorded_monitor_gaps':gaps,'cash_reconciliation':recon,
            'cycle':last['scan'] if last else None,'cycle_at':last['ts'] if last else None,
            'screened':screened,'in_range':in_range,'qualified_in_cycle':qualified,'unknown_gates':dict(missing.most_common()),
            'failed_gates':dict(failed.most_common()),'tape_checks':probes,'performance':forward_results(c,s,book)}

"""Read-only public market feeds. Unknowns are never silently replaced by zero."""
import os, math, time, json, base64, hashlib, re, threading
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor,as_completed
import requests
from pathlib import Path
from io import BytesIO
from urllib.parse import urlsplit
from PIL import Image, ImageOps

DEX='https://api.dexscreener.com'; GECKO='https://api.geckoterminal.com/api/v2/networks/solana'; RUG='https://api.rugcheck.xyz/v1/tokens'; RPC=os.getenv('SOLANA_RPC_URL','https://api.mainnet-beta.solana.com'); TRACKER=os.getenv('SOLANA_TRACKER_API_KEY','')
ABC='123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'; ADDR=re.compile(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$'); PROGRAMS={'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA','TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'}
class Missing(Exception):pass
def n(x):
    if x is None or isinstance(x,bool):return None
    try:v=float(x);return v if math.isfinite(v) else None
    except (ValueError,TypeError,OverflowError):return None
def get(o,*keys):
    for k in keys:
        if not isinstance(o,dict):return None
        o=o.get(k)
    return o
def b58(b):
    v=int.from_bytes(b,'big');s=''
    while v:v,r=divmod(v,58);s=ABC[r]+s
    return '1'*(len(b)-len(b.lstrip(b'\0')))+s
def isoepoch(s):return datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()

def retry_delay(header,now=None):
    now=time.time() if now is None else now;seconds=n(header)
    if seconds is None and header:
        try:seconds=parsedate_to_datetime(header).timestamp()-now
        except (TypeError,ValueError,OverflowError):pass
    # Never retry earlier than an explicit provider deadline, even when >4 min.
    return max(20,seconds if seconds is not None else 60)

class Feeds:
    def __init__(self):
        self.health={};self.lock=threading.Lock(); self.cache={};self.since={};self.next_allowed={};self.omissions=[]
    def health_note(self,src,ok,reason=''):
        with self.lock:
            if ok and time.time()<self.next_allowed.get(src,0):return
            self.health[src]={'ok':ok,'at':time.time(),'note':reason[:160]}
    def fetch(self,src,url,body=None,ttl=0,timeout=7,headers=None,max_age=240):
        key=(src,url,json.dumps(body,sort_keys=True) if body else '');now=time.time()
        with self.lock:
            cached=self.cache.get(key)
            if cached and 0<=now-cached[0]<ttl and cached[2]+now-cached[0]<=max_age:return cached[1]
            if now<self.next_allowed.get(src,0):raise Missing(src+' is in provider backoff')
            wait=0.
            if src=='GeckoTerminal':
                clock=time.monotonic();slot=max(clock,self.since.get(src,clock));wait=slot-clock
                if wait>8:raise Missing(src+' request queue is full')
                self.since[src]=slot+3.0
        if wait:
            time.sleep(wait)
            with self.lock:
                if time.time()<self.next_allowed.get(src,0):raise Missing(src+' is in provider backoff')
        try:
            opts={'timeout':(3,timeout),'headers':{'Accept':'application/json','User-Agent':'NightshiftPaperDesk/1.0',**(headers or {})}};r=requests.post(url,json=body,**opts) if body else requests.get(url,**opts)
            if r.status_code==429:
                with self.lock:self.next_allowed[src]=max(self.next_allowed.get(src,0),time.time()+retry_delay(r.headers.get('Retry-After')))
            if not r.ok:raise Missing(src+' HTTP '+str(r.status_code))
            age=n(r.headers.get('Age')) or 0
            if r.headers.get('Date'):
                try:age=max(age,time.time()-parsedate_to_datetime(r.headers['Date']).timestamp())
                except (ValueError,TypeError):pass
            if age>max_age:raise Missing(src+' returned a stale response')
            d=r.json()
            if (body or src in ('RugCheck','TrenchBot','SolanaTracker')) and isinstance(d,dict) and d.get('error'):raise Missing(src+' returned a provider error')
            self.health_note(src,True)
            if ttl:
                with self.lock:
                    if len(self.cache)>800:self.cache={k:v for k,v in self.cache.items() if now-v[0]<600}
                    self.cache[key]=(time.time(),d,age)
            return d
        except Missing as e:self.health_note(src,False,str(e));raise
        except Exception as e:
            if src=='TrenchBot' and isinstance(e,requests.exceptions.Timeout):
                with self.lock:self.next_allowed[src]=time.time()+75
            msg=src+' '+('timed out' if isinstance(e,requests.exceptions.Timeout) else 'unavailable');self.health_note(src,False,msg);raise Missing(msg) from None
    def rpc(self,method,params):
        d=self.fetch('Solana RPC',RPC,{'jsonrpc':'2.0','id':1,'method':method,'params':params},timeout=12)
        if 'result' not in d:raise Missing('No confirmed RPC result')
        return d['result']
    def scout(self,retained=()):
        urls=[('DexScreener',DEX+'/token-profiles/latest/v1'),('DexScreener',DEX+'/token-boosts/latest/v1'),('DexScreener',DEX+'/token-boosts/top/v1'),('GeckoTerminal',GECKO+'/trending_pools?page=1'),('GeckoTerminal',GECKO+'/trending_pools?page=2'),('GeckoTerminal',GECKO+'/new_pools?page=1')]
        seeds=[];errors=[]
        def pull(item):
            src,url=item;o=self.fetch(src,url,ttl=45)
            if src=='DexScreener':return [p.get('tokenAddress') for p in o if p.get('chainId')=='solana']
            return [(get(p,'relationships','base_token','data','id') or '').removeprefix('solana_') for p in o['data']]
        with ThreadPoolExecutor(max_workers=4) as pool:
            for f in [pool.submit(pull,it) for it in urls]:
                try:seeds+=f.result()
                except Exception as e:errors.append(str(e)[:160])
        seeds=list(dict.fromkeys(k for k in seeds+list(retained) if k and ADDR.fullmatch(k)))[:210];pairs={}
        for i in range(0,len(seeds),30):
            try:
                for raw in self.fetch('DexScreener',DEX+'/tokens/v1/solana/'+','.join(seeds[i:i+30]),ttl=15):
                    p=self.normalize(raw)
                    if p and p['mint'] in seeds:pairs[p['pair']]=p
            except Exception as e:errors.append(str(e)[:160])
        self.omissions=sorted(set(seeds)-{p['mint'] for p in pairs.values()})
        return list(pairs.values()),list(dict.fromkeys(errors))
    def normalize(self,r):
        pair=r.get('pairAddress','');mint=get(r,'baseToken','address') or ''
        if r.get('chainId')!='solana' or not ADDR.fullmatch(pair) or not ADDR.fullmatch(mint):return None
        t=n(r.get('pairCreatedAt'));return {'pair':pair,'mint':mint,'symbol':str(get(r,'baseToken','symbol') or '?')[:32],'name':str(get(r,'baseToken','name') or 'Unnamed pair')[:100],'created':t/1000 if t else None,'mcap':n(r.get('marketCap')),'liq':n(get(r,'liquidity','usd')),'vol':n(get(r,'volume','h1')),'change':n(get(r,'priceChange','h1')),'price':n(r.get('priceUsd')),'dex':str(r.get('dexId','unknown'))[:50],'buys':n(get(r,'txns','h1','buys')),'sells':n(get(r,'txns','h1','sells')),'observed':time.time(),'icon':get(r,'info','imageUrl')}
    def direct_pair(self,p):
        d=self.fetch('DexScreener',DEX+'/latest/dex/pairs/solana/'+p['pair']);a=next((self.normalize(x) for x in d.get('pairs',[]) if x.get('pairAddress')==p['pair'] and get(x,'baseToken','address')==p['mint']),None)
        if not a:raise Missing('Exact market quote unavailable')
        return a
    def report(self,mint):
        d=self.fetch('RugCheck',RUG+'/'+mint+'/report',ttl=80)
        if d.get('mint')!=mint:raise Missing('RugCheck mint mismatch')
        return d
    def mint(self,mint):
        d=self.rpc('getAccountInfo',[mint,{'encoding':'jsonParsed','commitment':'confirmed'}]);a=d.get('value') or {};info=get(a,'data','parsed','info')
        if a.get('owner') not in PROGRAMS or get(a,'data','parsed','type')!='mint' or not isinstance(info,dict) or info.get('isInitialized') is not True:raise Missing('Mint account could not be verified')
        return info,a['owner'],get(d,'context','slot')
    def owners(self,mint,program,supply,slot):
        # Ask the node for all accounts, not its top 20. Aggregate separate accounts owned by one wallet.
        # Supply reconciliation catches incomplete pages, inconsistent snapshots, and non-account data.
        d=self.rpc('getProgramAccounts',[program,{'commitment':'confirmed','minContextSlot':slot,'encoding':'base64','withContext':True,'dataSlice':{'offset':32,'length':40},'filters':[{'memcmp':{'offset':0,'bytes':mint}}]}]); owners={}; total=0
        for item in d['value']:
            a=item['account'];b=base64.b64decode(a['data'][0],validate=True)
            if len(b)!=40 or a['owner']!=program or a.get('space',0)<165:raise Missing('Unrecognized token-account layout')
            amt=int.from_bytes(b[32:40],'little');total+=amt
            if amt:owners[b[:32]]=owners.get(b[:32],0)+amt
        if total!=int(supply) or total<=0:raise Missing('Wallet balances do not reconcile to mint supply')
        return {b58(k):v for k,v in owners.items()},total,d['context']['slot']
    def thumbnail(self,p):
        # Decorative only. No arbitrary token-metadata hosts, redirects, or original image bytes served.
        url=p.get('icon') or '';base=Path(__file__).resolve().parent/'public'/'assets'/'tokens';dest=base/(p['mint']+'.webp')
        if dest.is_file() or not ADDR.fullmatch(p['mint']) or urlsplit(url).scheme!='https' or urlsplit(url).hostname!='cdn.dexscreener.com':return
        try:
            with requests.get(url,timeout=(2,3),stream=True,allow_redirects=False) as r:
                if r.status_code!=200:return
                raw=bytearray()
                for chunk in r.iter_content(65536):
                    raw.extend(chunk)
                    if len(raw)>1_500_000:return
            with Image.open(BytesIO(raw)) as im:
                if im.width*im.height>4000000:return
                base.mkdir(parents=True,exist_ok=True);thumb=ImageOps.fit(im.convert('RGB'),(80,80));tmp=dest.with_suffix('.tmp');thumb.save(tmp,'WEBP',quality=84);tmp.replace(dest)
        except Exception:return
    def tracker_history(self,creator,mint):
        # Require a complete, paginated, creator-attributed result; never infer clean from a null list.
        if not TRACKER or not ADDR.fullmatch(creator or ''):raise Missing('History unavailable')
        allrows=[];pages=None;total=None
        for page in range(1,4):
            url='https://data.solanatracker.io/deployer/'+creator+f'?format=full&limit=100&page={page}'
            d=self.fetch('SolanaTracker',url,ttl=120,headers={'x-api-key':TRACKER})
            if d.get('status')!='success' or not isinstance(d.get('data'),list) or n(d.get('pages')) is None or n(d.get('total')) is None or d.get('page')!=page:raise Missing('Incomplete deployment index')
            if pages is None:pages=d['pages'];total=d['total']
            if pages!=d['pages'] or total!=d['total'] or pages>3 or pages<1:raise Missing('Deployment index exceeds verification budget')
            allrows.extend(d['data'])
            if page==pages:break
        if len(allrows)!=total:raise Missing('Deployment pagination incomplete')
        seen={}
        for row in allrows:
            m=get(row,'token','mint');flag=get(row,'risk','rugged');owner=get(row,'token','creation','creator')
            if not ADDR.fullmatch(m or '') or owner!=creator or not isinstance(flag,bool):raise Missing('Creator attribution or prior rug verdict missing')
            seen[m]=seen.get(m,False) or flag
        if mint not in seen:raise Missing('Current launch not in creator index')
        return seen
    def bundle(self,p):
        def consistent(value,count):
            v=n(value);k=n(count)
            return v is not None and k is not None and 0<=v<=100 and k>=0 and k.is_integer() and ((v==0 and k==0) or (v>0 and k>0))
        if TRACKER:
            try:
                r=self.fetch('SolanaTracker','https://data.solanatracker.io/tokens/'+p['mint']+'/bundlers',ttl=120,headers={'x-api-key':TRACKER})
                if consistent(r.get('initialPercentage'),r.get('total')):return float(r['initialPercentage']),'SolanaTracker · initial bundles'
            except Missing:pass
        if not p['mint'].endswith('pump'):raise Missing('No launch-bundle index for this mint')
        r=self.fetch('TrenchBot','https://trench.bot/api/bundle/bundle_advanced/'+p['mint'],ttl=180,timeout=4)
        if not isinstance(r,dict) or r.get('mint',p['mint'])!=p['mint'] or not consistent(r.get('total_percentage_bundled'),r.get('total_bundles')):raise Missing('No complete launch-bundle result')
        return float(r['total_percentage_bundled']),'TrenchBot · initial bundles'
    def trades(self,p):
        r=self.fetch('GeckoTerminal',GECKO+'/pools/'+p['pair']+'/trades',ttl=12,max_age=110);out=[]
        for obj in r['data']:
            a=obj['attributes'];kind=a.get('kind');side='to' if kind=='buy' else 'from'
            if kind not in ('buy','sell') or a.get(side+'_token_address')!=p['mint'] or a.get('is_anomalous') is True:continue
            try:t=isoepoch(a['block_timestamp']);price=n(a['price_'+side+'_in_usd']);qty=n(a[side+'_token_amount']);usd=n(a['volume_in_usd']);wallet=a['tx_from_address'];tx=a['tx_hash']
            except (KeyError,TypeError,ValueError):continue
            if price and qty and usd and min(price,qty,usd)>0 and ADDR.fullmatch(wallet or '') and tx and 0<=time.time()-t<=3600:out.append({'tx':tx,'wallet':wallet,'ts':t,'side':kind,'qty':qty,'usd':usd,'price':price,'mint':p['mint']})
        return sorted(out,key=lambda x:x['ts'],reverse=True)
    def buyers(self,p):
        d=self.fetch('GeckoTerminal',GECKO+'/pools/'+p['pair'],ttl=30)['data'];m=(get(d,'relationships','base_token','data','id') or '').removeprefix('solana_')
        if d.get('id')!='solana_'+p['pair'] or m!=p['mint']:raise Missing('Pool or mint differs from indexed buyers')
        v=n(get(d,'attributes','transactions','h1','buyers'))
        if v is None or v<0:raise Missing('Unique-buyer count unavailable')
        return v
    def quote(self,p):
        trades=self.trades(p)
        if not trades or time.time()-trades[0]['ts']>90:raise Missing('No matching swap within 90 seconds')
        return trades[0]

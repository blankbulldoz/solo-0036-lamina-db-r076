#!/usr/bin/env python3
"""Bounded production walkthroughs using only the public HTTP service."""
import argparse, concurrent.futures, json, os, pathlib, shutil, socket, subprocess, tempfile, time, urllib.request, urllib.error
WORKFLOWS=['catalog-create','txn-write','snapshot-read','index-build','query-plan','sql-execute','txn-conflict','patch-merge','restart','checkpoint','backup-restore','changes','vacuum','transaction-close','statistics']
class Session:
 def __init__(self,binary,root,restore=None):
  self.binary=str(binary);self.root=pathlib.Path(root);self.restore=restore;self.process=None;self.log=None
 def start(self):
  with socket.socket() as sock:
   sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
  self.url=f'http://127.0.0.1:{port}/v1/lamina'
  args=[self.binary,'-data',str(self.root),'-listen',f'127.0.0.1:{port}']
  if self.restore:args+=['-restore',str(self.restore)];self.restore=None
  self.log=tempfile.TemporaryFile();self.process=subprocess.Popen(args,stdout=self.log,stderr=self.log)
  deadline=time.monotonic()+8
  while time.monotonic()<deadline:
   if self.process.poll() is not None:
    self.log.seek(0);raise RuntimeError(self.log.read().decode())
   try:self.call('GET','/health');return self
   except (OSError,urllib.error.URLError):time.sleep(.03)
  raise RuntimeError('service did not become ready')
 def stop(self):
  if self.process and self.process.poll() is None:
   self.process.terminate()
   try:self.process.wait(timeout=12)
   except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
  if self.log:self.log.close();self.log=None
 def call(self,method,path,data=None,status=200,raw=False):
  encoded=None if data is None else json.dumps(data,ensure_ascii=False).encode()
  req=urllib.request.Request(self.url+path,data=encoded,method=method,headers={'Content-Type':'application/json'})
  try:
   with urllib.request.urlopen(req,timeout=12) as response:code=response.status;body=response.read()
  except urllib.error.HTTPError as e:code=e.code;body=e.read()
  if code!=status:raise RuntimeError(f'{method} {path}: expected {status}, got {code}: {body.decode()}')
  return body if raw else json.loads(body)
 def begin(self,readonly=False):return self.call('POST','/transactions',{'read_only':readonly},201)['id']
 def put(self,tx,row,status=200,insert=False):return self.call('POST',f'/transactions/{tx}/put',{'relation':'spectra','row':row,'insert_only':insert},status)
 def patch(self,tx,key,set=None,expect=None,status=200):return self.call('POST',f'/transactions/{tx}/patch',{'relation':'spectra','key':key,'set':set or {},'expect':expect or {}},status)
 def commit(self,tx,status=200):return self.call('POST',f'/transactions/{tx}/commit',status=status)
 def query(self,**kwargs):return self.call('POST','/query',{'relation':'spectra',**kwargs})
 def stats(self):return self.call('GET','/stats')
def require(condition,message):
 if not condition:raise RuntimeError(message)
def tuple_(n,band=None,amplitude=None):return {'sample':n,'band':band or f'b{n}','amplitude':n*10 if amplitude is None else amplitude,'valid':True}
def seed(s):
 schema={'name':'spectra','primary':'sample','columns':[{'name':'sample','type':'int'},{'name':'band','type':'text'},{'name':'amplitude','type':'int','nullable':True},{'name':'valid','type':'bool'}]}
 s.call('POST','/relations',schema,201)
 tx=s.begin()
 for n in range(1,7):s.put(tx,tuple_(n))
 s.put(tx,{'sample':7,'band':'b7','amplitude':None,'valid':False})
 return s.commit(tx)['sequence']
def walkthrough(name,binary):
 with tempfile.TemporaryDirectory(prefix='lamina-flow-') as temp:
  temp=pathlib.Path(temp);s=Session(binary,temp/'source').start();other=None
  try:
   seed_seq=seed(s);observations={}
   if name=='catalog-create':
    before=s.stats()['sequence'];bad={'name':'invalid','primary':'missing','columns':[{'name':'value','type':'int'}]}
    s.call('POST','/relations',bad,400);require(s.stats()['sequence']==before,'invalid schema changed sequence')
    cat=s.call('GET','/catalog');require(len(cat['relations'])==1,'catalog mismatch');observations=cat
   elif name=='txn-write':
    tx=s.begin();s.put(tx,tuple_(8));s.put(tx,tuple_(9));require(s.query()['matched']==7,'uncommitted tuples leaked')
    result=s.commit(tx);require(result['writes']==2 and s.query()['matched']==9,'atomic commit failed')
    bad=s.begin();s.put(bad,{'sample':10,'band':'x','amplitude':'bad','valid':True},400)
    s.call('POST',f'/transactions/{bad}/rollback');observations=result
   elif name=='snapshot-read':
    old=s.begin();writer=s.begin();s.put(writer,tuple_(1,amplitude=555));s.commit(writer)
    q={'where':[{'column':'sample','op':'=','value':1}]}
    require(s.query(transaction=old,**q)['rows'][0]['amplitude']==10,'repeatable read violated')
    require(s.query(**q)['rows'][0]['amplitude']==555,'current snapshot stale')
    s.put(old,tuple_(8));require(s.query(transaction=old)['matched']==8,'read-own-write scan missing')
    require(s.query(at=seed_seq)['rows'][0]['amplitude']==10,'time travel failed')
    s.call('POST',f'/transactions/{old}/rollback');s.call('POST','/query',{'relation':'spectra','at':999},410);observations={'old':10,'current':555}
   elif name=='index-build':
    definition={'name':'by_band','relation':'spectra','column':'band','unique':True}
    s.call('POST','/indexes',definition,201)
    a=s.begin();b=s.begin();s.put(a,tuple_(8,band='shared'));s.put(b,tuple_(9,band='shared'));s.commit(a);s.commit(b,409)
    swap=s.begin();s.put(swap,tuple_(1,band='b2'));s.put(swap,tuple_(2,band='b1'));s.commit(swap)
    duplicate=s.begin();s.put(duplicate,tuple_(10,band='b3'));s.commit(duplicate,409)
    observations=s.call('GET','/catalog')
   elif name=='query-plan':
    s.call('POST','/indexes',{'name':'by_amplitude','relation':'spectra','column':'amplitude'},201)
    at=s.stats()['sequence'];tx=s.begin();s.put(tx,tuple_(1,amplitude=999));s.commit(tx)
    q={'where':[{'column':'amplitude','op':'<=','value':20}]}
    fast=s.query(**q);slow=s.query(force_scan=True,**q)
    require(fast['rows']==slow['rows'] and fast['plan']['access']=='index','index/scan mismatch')
    historical=s.query(at=at,**q);require(len(historical['rows'])==2,'historical index lost prior value')
    observations={'indexed':fast,'historical':historical}
   elif name=='sql-execute':
    sql=s.call('POST','/sql',{'sql':'SELECT sample, amplitude FROM spectra WHERE amplitude >= 30 AND valid = true ORDER BY amplitude DESC LIMIT 2;'})
    structured=s.query(columns=['sample','amplitude'],where=[{'column':'amplitude','op':'>=','value':30},{'column':'valid','op':'=','value':True}],order=[{'column':'amplitude','desc':True}],limit=2)
    require(sql['rows']==structured['rows'],'SQL and structured query diverged')
    count=s.call('POST','/sql',{'sql':'SELECT count(amplitude) FROM spectra;'})
    total=s.call('POST','/sql',{'sql':'SELECT sum(amplitude) FROM spectra;'})
    require(count['rows']==[{'count':6}] and total['rows']==[{'sum':210}],'aggregate semantics wrong')
    s.call('POST','/sql',{'sql':'SELECT * FROM spectra; SELECT * FROM spectra;'},400);observations={'projection':sql['rows'],'count':count['rows'],'sum':total['rows']}
   elif name=='txn-conflict':
    a=s.begin();b=s.begin();s.put(a,tuple_(1,amplitude=101));s.put(b,tuple_(1,amplitude=202));s.put(b,tuple_(8))
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
     jobs=[pool.submit(s.commit,a),pool.submit(s.commit,b)]
     results=[]
     for job in jobs:
      try:results.append({'commit':job.result()})
      except RuntimeError as e:
       require('got 409' in str(e),'unexpected concurrent failure');results.append({'conflict':True})
    require(sum('conflict' in r for r in results)==1,'exactly one writer must lose')
    rows=s.query()['rows'];one=next(r for r in rows if r['sample']==1)
    require((one['amplitude']==202)==any(r['sample']==8 for r in rows),'conflicting commit partially published')
    s.commit(a,404);s.commit(b,404);observations={'results':results,'rows':rows}
   elif name=='patch-merge':
    s.call('POST','/indexes',{'name':'by_band','relation':'spectra','column':'band','unique':True},201)
    reader=s.begin();a=s.begin();b=s.begin()
    s.patch(a,1,set={'amplitude':111});s.patch(b,1,set={'valid':False},expect={'band':'b1'})
    first=s.commit(a);second=s.commit(b)
    require(first.get('merged',0)==0 and second.get('merged',0)==1,'disjoint patches did not reconcile')
    row=next(r for r in s.query()['rows'] if r['sample']==1)
    require(row['amplitude']==111 and row['valid']==False and row['band']=='b1','merged row lost a column intent')
    pinned=next(r for r in s.query(transaction=reader)['rows'] if r['sample']==1)
    require(pinned['amplitude']==10 and pinned['valid']==True,'snapshot reader saw merged state')
    feed=s.call('GET',f'/changes?after={seed_seq}&limit=100')
    logged=[w['row'] for r in feed['records'] for w in r.get('writes',[]) if w['key']=='int:1' and w.get('row')]
    require(logged and logged[-1]==row,'journal did not record the merged final row')
    c=s.begin();d=s.begin()
    s.patch(c,1,set={'amplitude':500});s.patch(d,1,set={'amplitude':900})
    s.commit(c);s.commit(d,409)
    require(next(r for r in s.query()['rows'] if r['sample']==1)['amplitude']==500,'losing same-column patch leaked')
    e=s.begin();f=s.begin()
    s.patch(e,1,set={'band':'rx'},expect={'amplitude':500})
    s.patch(f,1,set={'amplitude':600});s.commit(f);s.commit(e,409)
    e2=s.begin();s.patch(e2,1,set={'band':'rx'},expect={'amplitude':600})
    require(s.commit(e2).get('merged',0)==0,'clean re-read patch reported a merge')
    g=s.begin();h=s.begin()
    s.patch(g,3,set={'band':'shared'});s.patch(h,4,set={'band':'shared'})
    s.commit(g);s.commit(h,409)
    require(next(r for r in s.query()['rows'] if r['sample']==4)['band']=='b4','unique loser overwrote band')
    i=s.begin();k=s.begin()
    s.put(i,tuple_(1,amplitude=777));s.patch(k,1,set={'valid':True})
    s.commit(k);s.commit(i,409)
    u=s.begin();v=s.begin()
    s.patch(u,5,set={'amplitude':555});s.patch(v,5,set={'band':'v5'})
    s.commit(u);rv=s.commit(v)
    require(rv.get('merged',0)==1,'second disjoint merge did not reconcile')
    row5=next(r for r in s.query()['rows'] if r['sample']==5)
    require(row5['amplitude']==555 and row5['band']=='v5','second merge lost intent')
    m=s.begin();head=s.stats()['sequence']
    s.patch(m,1,expect={'band':'rx'});asserted=s.commit(m)
    require(asserted['writes']==0 and s.stats()['sequence']==head,'assert-only patch published a version')
    n=s.begin();s.patch(n,8,set={'band':'b8','amplitude':80,'valid':False});s.commit(n)
    o=s.begin();p=s.begin()
    s.patch(o,9,set={'band':'b9','amplitude':90,'valid':True});s.patch(p,9,set={'band':'b9x','amplitude':91,'valid':True})
    s.commit(o);s.commit(p,409)
    require(s.query()['matched']==9,'patch-created rows missing')
    require(s.query(transaction=reader)['matched']==7,'pinned reader lost rows')
    s.call('POST',f'/transactions/{reader}/rollback')
    x=s.begin();y=s.begin()
    s.patch(x,2,set={'amplitude':222})
    s.call('POST',f'/transactions/{y}/delete',{'relation':'spectra','key':2})
    s.commit(y);s.commit(x,409)
    require(all(r['sample']!=2 for r in s.query()['rows']),'patch resurrected a deleted row')
    own=s.begin();s.patch(own,1,set={'amplitude':4242})
    require(next(r for r in s.query(transaction=own)['rows'] if r['sample']==1)['amplitude']==4242,'transaction cannot read its own staged patch')
    s.call('POST',f'/transactions/{own}/rollback')
    comp=s.begin()
    s.patch(comp,6,set={'amplitude':66});s.patch(comp,6,set={'valid':False},expect={'band':'b6'})
    s.put(comp,tuple_(6,amplitude=606));s.patch(comp,6,set={'band':'c6'})
    s.commit(comp)
    row6=next(r for r in s.query()['rows'] if r['sample']==6)
    require(row6['amplitude']==606 and row6['band']=='c6' and row6['valid']==True,'staged put/patch composition wrong')
    comp2=s.begin()
    s.patch(comp2,7,set={'amplitude':70});s.patch(comp2,7,set={'valid':True},expect={'band':'b7'})
    s.commit(comp2)
    row7=next(r for r in s.query()['rows'] if r['sample']==7)
    require(row7['amplitude']==70 and row7['valid']==True,'staged patch merge wrong')
    stale=s.begin();s.patch(stale,1,set={'valid':True},expect={'amplitude':999},status=409)
    s.call('POST',f'/transactions/{stale}/rollback')
    ro=s.begin(readonly=True);s.patch(ro,1,set={'amplitude':1},status=409);s.call('POST',f'/transactions/{ro}/rollback')
    s.patch('deadbeef',1,set={'amplitude':1},status=404)
    bad=s.begin()
    s.patch(bad,1,set={'sample':5},status=400);s.patch(bad,1,set={'nosuch':1},status=400)
    s.patch(bad,1,set={'amplitude':'bad'},status=400)
    s.patch(bad,1,set={'band':'x'},expect={'band':'y'},status=400)
    s.patch(bad,42,set={'amplitude':5},expect={'band':'y'},status=404)
    s.call('POST',f'/transactions/{bad}/delete',{'relation':'spectra','key':2})
    s.patch(bad,2,set={'band':'x'},status=400)
    s.call('POST',f'/transactions/{bad}/rollback')
    merges=s.stats()['merges_since_open']
    require(merges>=2,'merge counter missing')
    before=s.query()['rows']
    backup=s.call('GET','/backup',raw=True);bpath=temp/'merged.lmi';bpath.write_bytes(backup)
    s.call('POST','/checkpoint')
    s.stop();s.start()
    require(s.query()['rows']==before,'checkpoint plus suffix changed merged rows')
    other=Session(binary,temp/'restored',bpath).start()
    require(other.query()['rows']==before,'backup restore diverged from merged fact')
    other.stop();other=None
    require(s.stats()['merges_since_open']==0,'process counter survived restart')
    observations={'merged_commit':second,'row':row,'assert_only':asserted,'merges':merges}
   elif name=='restart':
    before=s.query()['rows'];s.stop()
    with (s.root/'journal.lmw').open('ab') as f:f.write(b'\x00\x00\x00')
    s.start();require(s.query()['rows']==before,'tail recovery changed committed data');require(s.stats()['recovered_tail_bytes']==3,'tail recovery not reported')
    s.stop();broken=temp/'broken';shutil.copytree(s.root,broken)
    data=bytearray((broken/'journal.lmw').read_bytes());data[-1]^=1;(broken/'journal.lmw').write_bytes(data)
    result=subprocess.run([str(binary),'-data',str(broken),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(result.returncode!=0 and 'checksum' in result.stderr,'full frame corruption accepted')
    observations={'tail_bytes':3,'corruption':'rejected'}
   elif name=='checkpoint':
    at=s.call('POST','/checkpoint')['sequence'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    before=s.query()['rows'];s.stop();s.start();require(s.query()['rows']==before,'checkpoint plus suffix recovery failed')
    locked=subprocess.run([str(binary),'-data',str(s.root),'-listen','127.0.0.1:0'],capture_output=True,text=True,timeout=5)
    require(locked.returncode!=0 and 'already open' in locked.stderr,'exclusive directory ownership missing');observations={'checkpoint':at,'head':s.stats()['sequence']}
   elif name=='backup-restore':
    backup=s.call('GET','/backup',raw=True);path=temp/'portable.lmi';path.write_bytes(backup)
    frozen=s.query()['rows'];tx=s.begin();s.put(tx,tuple_(8));s.commit(tx)
    other=Session(binary,temp/'restored',path).start();require(other.query()['rows']==frozen,'backup mutated with source')
    tx=other.begin();other.put(tx,tuple_(9));result=other.commit(tx);other.stop();other.start()
    require(other.query()['matched']==8,'restored journal base does not continue')
    other.call('GET','/changes?after=0',status=410)
    broken=json.loads(backup);broken['sha256']='0'*64;bad=temp/'bad.lmi';bad.write_text(json.dumps(broken))
    failed=subprocess.run([str(binary),'-restore',str(bad),'-data',str(temp/'badrestore')],capture_output=True,text=True,timeout=5)
    require(failed.returncode!=0 and not (temp/'badrestore').exists(),'invalid backup published');observations={'restored_commit':result,'original_sequence':seed_seq}
   elif name=='changes':
    tx=s.begin()
    for n in range(8,12):s.put(tx,tuple_(n))
    committed=s.commit(tx);page=s.call('GET',f'/changes?after={seed_seq}&limit=1')
    require(len(page['records'])==1 and len(page['records'][0]['writes'])==4 and page['cursor']==committed['sequence'],'change page split transaction')
    s.call('GET','/changes?after=999',status=400);observations=page
   elif name=='vacuum':
    old=s.begin();tx=s.begin();s.put(tx,tuple_(1,amplitude=99));s.commit(tx);requested=s.stats()['sequence']
    pinned=s.call('POST','/vacuum',{'horizon':requested});require(pinned['pinned'] and pinned['horizon']==seed_seq,'vacuum crossed active snapshot')
    require(s.query(transaction=old)['rows'][0]['amplitude']==10,'active snapshot lost')
    s.call('POST',f'/transactions/{old}/rollback');reclaimed=s.call('POST','/vacuum',{'horizon':requested})
    require(reclaimed['removed']>=1,'obsolete version not reclaimed');s.call('POST','/query',{'relation':'spectra','at':seed_seq},410)
    s.call('POST','/checkpoint');s.stop();s.start();require(s.query()['rows'][0]['amplitude']==99,'vacuum did not survive restart');observations={'pinned':pinned,'reclaimed':reclaimed}
   elif name=='transaction-close':
    tx=s.begin();s.put(tx,tuple_(8));s.call('POST',f'/transactions/{tx}/rollback');s.commit(tx,404)
    require(s.query()['matched']==7 and not s.stats()['active'],'rollback leaked state or pin')
    tx=s.begin(True);s.put(tx,tuple_(9),409);s.call('POST',f'/transactions/{tx}/rollback');observations={'rollback':'discarded','read_only':'enforced'}
   elif name=='statistics':
    s.call('POST','/indexes',{'name':'by_amp','relation':'spectra','column':'amplitude'},201)
    q=s.query(where=[{'column':'amplitude','op':'>=','value':40}],limit=2)
    require(q['matched']==3 and q['scanned']==3 and len(q['rows'])==2 and q['truncated'],'query fact meanings conflated')
    observations={'query':q,'stats':s.stats()}
   else:raise ValueError(name)
   return {'workflow':name,'status':'observed','observations':observations}
  finally:
   s.stop()
   if other:other.stop()
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--workflow',choices=['all',*WORKFLOWS],default='all');args=parser.parse_args()
 project=pathlib.Path(__file__).resolve().parents[1];binary=project/'bin/laminad'
 if not binary.exists():raise SystemExit('build first: go build -o bin/laminad ./cmd/laminad')
 names=WORKFLOWS if args.workflow=='all' else [args.workflow]
 for name in names:print(json.dumps(walkthrough(name,binary),ensure_ascii=False),flush=True)
if __name__=='__main__':main()

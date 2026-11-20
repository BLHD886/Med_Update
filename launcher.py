import sys,socket,threading,json,secrets
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError
if sys.version_info<(3,10):raise SystemExit("Python 3.10 or newer is required")
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'backend'))
from prd_service import PRDStore,handler,main
from service import load_token
from http.server import ThreadingHTTPServer
if '--check' in sys.argv:
 store=PRDStore(ROOT/'backend/runtime/prd.sqlite3')
 token=secrets.token_urlsafe(32)
 server=ThreadingHTTPServer(('127.0.0.1',0),handler(store,token,secrets.token_urlsafe(32)))
 t=threading.Thread(target=server.serve_forever,daemon=True);t.start()
 url='http://127.0.0.1:'+str(server.server_port)+'/v1/guardian/snapshot'
 try:
  try:urlopen(url,timeout=4);raise AssertionError('missing auth accepted')
  except HTTPError as e:assert e.code==401
  with urlopen(Request(url,headers={'Authorization':'Bearer '+token}),timeout=4) as res:data=json.load(res)
  assert len(data['tasks'])==3 and len(data['handoff'])==3
  assert all(t['state']=='RESOLVED' for t in data['tasks'])
  assert data['historical_dataset']['rows']==12
  assert data['paused'] and not data['ai_consent']
  print('PASS: HTTP 200, unauthorized 401, 12 samples, 3 resolved tasks, AI disabled.')
 finally:server.shutdown();server.server_close();t.join()
else:
 try:
  s=socket.socket();s.bind(('127.0.0.1',8768));s.close()
 except OSError:raise SystemExit('8768端口已占用。请关闭以前启动的后台窗口后重试；不要同时启动多份。')
 token=load_token(ROOT/'backend/runtime/parent-token.txt')
 print('后台将在本机127.0.0.1:8768运行。请保持此窗口打开。',flush=True)
 print('在Floatboat中展开“连接设置”，粘贴下面这一整行访问码：',flush=True)
 print(token,flush=True)
 print('如需再次查看，打开 backend/runtime/parent-token.txt。不要使用朋友原电脑的访问码。',flush=True)
 sys.argv=[sys.argv[0]]
 main()

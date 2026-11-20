"""PRD single-family simulation backend: evidence, bounded actions and explicit resolution."""
from service import Store as BaseStore, make_handler, load_token, stamp, encoded, ROOT
import json,sqlite3,time,uuid,threading,argparse
from http.server import ThreadingHTTPServer
from datetime import datetime,timezone

CONTACTS=[{'id':'primary','name':'模拟主要监护人','role':'caregiver','authorized':True},
          {'id':'backup','name':'模拟备用监护人','role':'caregiver','authorized':True},
          {'id':'admin','name':'模拟家庭管理员','role':'admin','authorized':True}]
POLICY={'version':'care-policy-v1','next_check_seconds':60,'max_followups':3,'max_backup':1,'backup_after_seconds':60,'channel':'simulated_inbox','identity':'照护协调助理','forbidden':['诊断','用药','改医疗阈值','自动完成任务','扩大权限']}

class PRDStore(BaseStore):
    def __init__(self,path):
        super().__init__(path)
        with self.db() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY,task_id TEXT,body TEXT);
            CREATE TABLE IF NOT EXISTS feedback(id TEXT PRIMARY KEY,task_id TEXT,body TEXT);
            CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,task_id TEXT,body TEXT);
            CREATE TABLE IF NOT EXISTS episodes(event_id TEXT PRIMARY KEY,body TEXT);
            CREATE TABLE IF NOT EXISTS proposals(id TEXT PRIMARY KEY,body TEXT);
            ''')
            row=db.execute("SELECT value FROM settings WHERE key='policy'").fetchone()
            self.policy=json.loads(row[0]) if row else dict(POLICY)

    def ingest(self,body):
        result=super().ingest(body)
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            for row in db.execute('SELECT id,body FROM tasks').fetchall():
                task=json.loads(row['body'])
                if 'policy_version' in task:continue
                task.update(policy_version=self.policy['version'],policy_snapshot=self.policy,backup_attempts=0,last_reply='',observation='',created_epoch=time.time(),resolved_at=None)
                db.execute('UPDATE tasks SET body=? WHERE id=?',(encoded(task),task['id']))
                event=json.loads(db.execute('SELECT body FROM inputs WHERE id=?',(task['event_id'],)).fetchone()[0])
                episode={'event_id':event['id'],'level':task['level'],'condition_started_at':event.get('started_at',event.get('observed_at',stamp())),'triggered_at':event.get('observed_at',stamp()),'condition_ended_at':None,'peak_value':event.get('value'),'source':'synthetic','is_demo':True}
                db.execute('INSERT OR IGNORE INTO episodes VALUES(?,?)',(event['id'],encoded(episode)))
                self.receipt(db,task,'base:'+task['event_id'],'primary','基础模拟提醒','base_reminder')
        return result

    def receipt(self,db,task,operation,recipient,text,kind):
        if recipient not in ('primary','backup'):raise ValueError('unauthorized recipient')
        old=db.execute('SELECT body FROM receipts WHERE id=?',(operation,)).fetchone()
        if old:return json.loads(old[0])
        flag=db.execute("SELECT value FROM settings WHERE key='simulate_notify_failure'").fetchone()
        item={'id':operation,'task_id':task['id'],'recipient':recipient,'kind':kind,'at':stamp(),'status':'failed' if flag and flag[0]=='true' else 'sent_to_simulated_inbox','text':text,'delivered_to_phone':False,'human_acknowledged':False}
        db.execute('INSERT INTO receipts VALUES(?,?,?)',(operation,task['id'],encoded(item)))
        self.audit(db,'notification_receipt',item)
        return item

    def end_episode(self,event_id,at):
        with self.db() as db:
            row=db.execute('SELECT body FROM episodes WHERE event_id=?',(event_id,)).fetchone()
            if not row:raise ValueError('episode not found')
            episode=json.loads(row[0]);episode['condition_ended_at']=at
            db.execute('UPDATE episodes SET body=? WHERE event_id=?',(encoded(episode),event_id));self.audit(db,'condition_recovered',episode)

    def feedback(self,body):
        if body.get('owner') not in ('primary','backup','admin'):raise ValueError('操作者必须为已授权联系人 primary/backup/admin')
        if body.get('state')=='RESOLVED' and body.get('explicit_complete') is not True:raise ValueError('必须明确提交完成及处理结果')
        result=super().feedback(body)
        with self.db() as db:
            if result['state']=='RESOLVED':result['resolved_at']=stamp()
            if result['state']=='NEEDS_HUMAN':result['owner']=body['owner']
            db.execute('UPDATE tasks SET body=? WHERE id=?',(encoded(result),result['id']))
        return result

    def reply(self,body):
        actor=body.get('actor');text=body.get('text');label=body.get('observation','')
        if actor not in ('primary','backup','admin') or not isinstance(text,str) or not 1<=len(text)<=2000:raise ValueError('invalid authorized reply')
        if label not in ('','误报','确认发作','不确定','漏报'):raise ValueError('invalid observation')
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT body,version FROM tasks WHERE id=?',(body.get('task_id'),)).fetchone()
            if not row:raise ValueError('task not found')
            task=json.loads(row['body'])
            if task['state']=='RESOLVED':raise ValueError('完成后新问题必须创建关联任务，不能覆盖原记录')
            item={'id':str(uuid.uuid4()),'task_id':task['id'],'actor':actor,'text':text,'observation':label,'at':stamp()}
            db.execute('INSERT INTO feedback VALUES(?,?,?)',(item['id'],task['id'],encoded(item)))
            task.update(last_reply=text,last_reply_actor=actor,observation=label or task.get('observation',''),version=row['version']+1,updated_at=stamp())
            db.execute('UPDATE tasks SET body=?,version=?,due=? WHERE id=?',(encoded(task),task['version'],time.time(),task['id']))
            self.audit(db,'caregiver_reply',item)
        return item

    def setting(self,key,value):
        if key=='simulate_notify_failure':
            if type(value) is not bool:raise ValueError('boolean required')
            with self.db() as db:db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',(key,json.dumps(value)));self.audit(db,'test_fault_injection',{key:value})
            return {'ok':True}
        return super().setting(key,value)

    def context(self,task):
        result=super().context(task)
        with self.db() as db:
            result['receipts']=[json.loads(r[0]) for r in db.execute('SELECT body FROM receipts WHERE task_id=?',(task['id'],))]
            result['feedback']=[json.loads(r[0]) for r in db.execute('SELECT body FROM feedback WHERE task_id=?',(task['id'],))]
            link=db.execute("SELECT value FROM settings WHERE key='data_link'").fetchone()
        result.update(contacts=CONTACTS,policy=task.get('policy_snapshot',POLICY),data_link=json.loads(link[0]) if link else {'status':'可用'},remaining_followups=max(0,3-task['attempts']),remaining_backup=max(0,1-task['backup_attempts']))
        return result

    def external_next(self):
        with self.db() as db:
            db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('external_heartbeat',str(time.time())))
            self.audit(db,'automatic_inspection',{'at':stamp(),'source':'floatboat_background_service'})
            flags=dict(db.execute('SELECT key,value FROM settings'))
            if flags.get('paused')=='true' or flags.get('ai_consent')!='true':return {'context':None}
            row=db.execute("SELECT body FROM tasks WHERE due<=? AND json_extract(body,'$.state') NOT IN ('RESOLVED','NEEDS_HUMAN') ORDER BY due LIMIT 1",(time.time(),)).fetchone()
        return {'context':self.context(json.loads(row[0])) if row else None}

    def tick(self,model=None,expected_task=None,expected_version=None,model_name=None):
        with self.db() as db:
            db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('heartbeat',stamp()))
            if expected_task is None:return
            flags=dict(db.execute('SELECT key,value FROM settings'))
            if flags.get('paused')=='true' or flags.get('ai_consent')!='true':return
            row=db.execute('SELECT body,version FROM tasks WHERE id=? AND version=? AND due<=?',(expected_task,expected_version,time.time())).fetchone()
            if not row:return
        task=json.loads(row['body']);context=self.context(task)
        if task['state'] in ('RESOLVED','NEEDS_HUMAN'):return
        error=None
        try:
            result=model(context)
            if not isinstance(result,dict):raise ValueError('JSON required')
            if result.get('task_id')!=task['id'] or result.get('event_id')!=task['event_id'] or result.get('policy_version')!=task['policy_version']:raise ValueError('identity/policy mismatch')
            if result.get('action') not in ('wait','follow_up','notify_backup','needs_human'):raise ValueError('unauthorized action')
            if result.get('reply_status') not in ('NONE','ACKNOWLEDGED','IN_PROGRESS'):raise ValueError('invalid reply interpretation')
            if result.get('evidence_ids')!=[task['event_id']]:raise ValueError('invalid evidence references')
            if type(result.get('next_check_seconds')) is not int or not 30<=result['next_check_seconds']<=300:raise ValueError('unbounded wait')
            for key in ('reason','summary','human_reason'):
                if not isinstance(result.get(key),str) or len(result[key])>2000:raise ValueError('missing decision field')
            if result['action']=='notify_backup' and (task['level']!=3 or time.time()-task['created_epoch']<60 or task.get('last_reply')):raise ValueError('backup escalation not authorized')
            recipient='backup' if result['action']=='notify_backup' else 'primary'
            if result.get('recipient')!=recipient:raise ValueError('unauthorized recipient')
        except Exception as exc:
            error=str(exc);result={'task_id':task['id'],'event_id':task['event_id'],'action':'needs_human' if task['failures']>=2 else 'wait','reason':'模型输出校验失败：'+error,'summary':'未执行越权或无效动作','human_reason':error,'next_check_seconds':60,'policy_version':task['policy_version'],'evidence_ids':[task['event_id']],'recipient':'primary','reply_status':'NONE'}
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            current=db.execute('SELECT version FROM tasks WHERE id=?',(task['id'],)).fetchone()
            flags=dict(db.execute('SELECT key,value FROM settings'))
            if current[0]!=expected_version or flags.get('paused')=='true' or flags.get('ai_consent')!='true':self.audit(db,'decision_discarded',{'task_id':task['id'],'reason':'暂停/撤回/人工修改'});return
            if error:task['failures']+=1
            if not error and task.get('last_reply'):
                if result['reply_status']=='IN_PROGRESS':task['state']='IN_PROGRESS';task['owner']=task.get('last_reply_actor','primary')
                elif result['reply_status']=='ACKNOWLEDGED' and task['state']=='OPEN':task['state']='ACKNOWLEDGED';task['owner']=task.get('last_reply_actor','primary')
            if result['action']=='follow_up':
                if task['attempts']>=3:result['action']='needs_human'
                else:
                    task['attempts']+=1
                    receipt=self.receipt(db,task,f"follow:{task['id']}:{task['attempts']}",'primary',result['reason'],'follow_up')
                    if receipt['status']=='failed':task['failures']+=1
            if result['action']=='notify_backup':
                if task['backup_attempts']>=1:result['action']='needs_human'
                else:task['backup_attempts']+=1;self.receipt(db,task,'backup:'+task['id'],'backup',result['reason'],'backup_escalation')
            if result['action']=='needs_human' or task['failures']>=3:task['state']='NEEDS_HUMAN'
            decision={'id':str(uuid.uuid4()),'task_id':task['id'],'at':stamp(),'model':model_name or 'test_double','input':context,'output':result,'error':error}
            db.execute('INSERT INTO decisions VALUES(?,?,?)',(decision['id'],task['id'],encoded(decision)))
            task.update(last_result=result['reason'],version=task['version']+1,updated_at=stamp(),next_check_at=datetime.fromtimestamp(time.time()+result['next_check_seconds'],timezone.utc).isoformat())
            db.execute('UPDATE tasks SET body=?,version=?,due=? WHERE id=?',(encoded(task),task['version'],time.time()+result['next_check_seconds'],task['id']))
            self.audit(db,'model_failure' if error else 'ai_decision',{'task_id':task['id'],'decision_id':decision['id'],'result':result,'model':model_name})

    def snapshot(self):
        result=super().snapshot()
        with self.db() as db:
            receipts=[json.loads(r[0]) for r in db.execute('SELECT body FROM receipts ORDER BY rowid DESC LIMIT 200')]
            feedback=[json.loads(r[0]) for r in db.execute('SELECT body FROM feedback ORDER BY rowid DESC LIMIT 200')]
            tasks={t['event_id']:t for t in result['tasks']};handoff=[]
            for r in db.execute('SELECT body FROM episodes ORDER BY rowid'):
                e=json.loads(r[0]);t=tasks.get(e['event_id'],{});e.update(task_id=t.get('id'),state=t.get('state'),owner=t.get('owner'),resolved_at=t.get('resolved_at'),outcome=t.get('note',''))
                start=datetime.fromisoformat(e['condition_started_at']).timestamp();trigger=datetime.fromisoformat(e['triggered_at']).timestamp();end=datetime.fromisoformat(e['condition_ended_at']).timestamp() if e['condition_ended_at'] else time.time()
                e['condition_duration_seconds']=max(0,end-start)
                e['alarm_duration_seconds']=max(0,(datetime.fromisoformat(e['resolved_at']).timestamp() if e['resolved_at'] else time.time())-trigger)
                handoff.append(e)
            link=db.execute("SELECT value FROM settings WHERE key='data_link'").fetchone()
            proposals=[json.loads(r[0]) for r in db.execute('SELECT body FROM proposals')]
        result.update(contacts=CONTACTS,policy=self.policy,receipts=receipts,feedback=feedback,handoff=handoff,data_link=json.loads(link[0]) if link else {'status':'可用'},proposals=proposals)
        return result

    def propose(self,body):
        changes=body.get('changes',{})
        if not isinstance(changes,dict) or not changes or set(changes)-{'next_check_seconds','message_text'}:raise ValueError('只允许非医疗流程修改；禁止改阈值、身份和权限')
        if 'next_check_seconds' in changes and (type(changes['next_check_seconds']) is not int or not 30<=changes['next_check_seconds']<=300):raise ValueError('invalid interval')
        if 'message_text' in changes and (not isinstance(changes['message_text'],str) or len(changes['message_text'])>500):raise ValueError('invalid text')
        item={'id':str(uuid.uuid4()),'changes':changes,'reason':str(body.get('reason',''))[:1000],'state':'PENDING','created_at':stamp(),'previous_policy':dict(self.policy)}
        with self.db() as db:db.execute('INSERT INTO proposals VALUES(?,?)',(item['id'],encoded(item)));self.audit(db,'policy_proposed',item)
        return item

    def approve(self,body):
        if body.get('actor')!='admin':raise ValueError('administrator approval required')
        with self.db() as db:
            row=db.execute('SELECT body FROM proposals WHERE id=?',(body.get('id'),)).fetchone()
            if not row:raise ValueError('proposal missing')
            item=json.loads(row[0])
            if body.get('rollback'):
                if item['state']!='APPROVED':raise ValueError('only approved policy can roll back')
                self.policy=item['previous_policy'];item['state']='ROLLED_BACK'
            else:
                if item['state']!='PENDING':raise ValueError('already processed')
                self.policy={**self.policy,**item['changes'],'version':'care-policy-'+uuid.uuid4().hex[:8]};item['state']='APPROVED'
            item['approved_by']='admin';item['processed_at']=stamp()
            db.execute('UPDATE proposals SET body=? WHERE id=?',(encoded(item),item['id']))
            db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('policy',encoded(self.policy)));self.audit(db,'policy_approval_or_rollback',item)
        return item

def handler(store,parent,device):
    Base=make_handler(store,parent,device)
    class Handler(Base):
        def do_POST(self):
            routes={'/v1/guardian/reply':store.reply,'/v1/guardian/proposal':store.propose,'/v1/guardian/approve-proposal':store.approve}
            if self.path not in routes:return super().do_POST()
            if not self.auth():return self.send(401,{'error':'unauthorized'})
            try:
                size=int(self.headers.get('Content-Length','0'))
                if not 0<size<10000:raise ValueError('invalid size')
                origin=self.headers.get('Origin')
                if origin:
                    from urllib.parse import urlsplit
                    if urlsplit(origin).netloc!=self.headers.get('Host'):return self.send(403,{'error':'cross-origin rejected'})
                return self.send(200,routes[self.path](json.loads(self.rfile.read(size))))
            except (ValueError,TypeError,KeyError) as e:return self.send(400,{'error':str(e)})
    return Handler

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--port',type=int,default=8768);args=parser.parse_args()
    runtime=ROOT/'runtime';runtime.mkdir(exist_ok=True);store=PRDStore(runtime/'prd.sqlite3')
    server=ThreadingHTTPServer(('127.0.0.1',args.port),handler(store,load_token(runtime/'parent-token.txt'),load_token(runtime/'device-token.txt')))
    stop=threading.Event()
    def heartbeat():
        while not stop.wait(10):store.tick()
    threading.Thread(target=heartbeat,daemon=True).start()
    print('PRD simulation service http://127.0.0.1:'+str(args.port),flush=True)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:stop.set();server.server_close()

if __name__=='__main__':main()

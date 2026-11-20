import unittest,tempfile,json,time,uuid
from pathlib import Path
from prd_service import PRDStore,POLICY

class PRDChecks(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.s=PRDStore(Path(self.tmp.name)/'test.sqlite3')
 def tearDown(self):self.tmp.cleanup()
 def create(self,level=2,key=None):
  e={'id':key or str(uuid.uuid4()),'level':level,'is_demo':True,'source':'synthetic','observed_at':'2026-11-19T13:00:00+00:00','started_at':'2026-11-19T12:59:57+00:00'}
  self.s.ingest({'records':[{'kind':'event','data':e}]});return self.s.snapshot()['tasks'][0],e
 def due(self):
  with self.s.db() as db:db.execute('UPDATE tasks SET due=0')
 def decision(self,task,action='follow_up',**extra):
  return {**{'task_id':task['id'],'event_id':task['event_id'],'policy_version':task['policy_version'],'evidence_ids':[task['event_id']],'summary':'测试上下文','action':action,'recipient':'backup' if action=='notify_backup' else 'primary','reply_status':'NONE','reason':'显式测试替身，不是真实模型','human_reason':'','next_check_seconds':60},**extra}
 def execute(self,result,task):
  self.s.setting('ai_consent',True);self.due();self.s.tick(lambda _:result,task['id'],task['version'],'explicit_test_double')
 def test_S1_no_event_no_task(self):self.s.tick();self.assertEqual(self.s.snapshot()['tasks'],[])
 def test_S2_base_before_ai_and_bounded_followup(self):
  t,e=self.create();self.assertEqual(len(self.s.snapshot()['receipts']),1)
  for _ in range(4):
   t=self.s.snapshot()['tasks'][0];self.execute(self.decision(t),t)
  t=self.s.snapshot()['tasks'][0];self.assertEqual(t['attempts'],3);self.assertEqual(t['state'],'NEEDS_HUMAN')
 def test_S3_backup_authorized_once(self):
  t,e=self.create(3);t['created_epoch']=time.time()-61
  with self.s.db() as db:db.execute('UPDATE tasks SET body=? WHERE id=?',(json.dumps(t),t['id']))
  self.execute(self.decision(t,'notify_backup'),t);t=self.s.snapshot()['tasks'][0];self.assertEqual(t['backup_attempts'],1);self.assertNotEqual(t['state'],'RESOLVED')
  self.execute(self.decision(t,'notify_backup'),t);self.assertEqual(self.s.snapshot()['tasks'][0]['backup_attempts'],1)
 def test_S3_early_or_unauthorized_backup_rejected(self):
  t,e=self.create(2);self.execute(self.decision(t,'notify_backup'),t);self.assertEqual(self.s.snapshot()['tasks'][0]['failures'],1)
 def test_S4_link_loss_preserves_alarm(self):
  t,e=self.create(3)
  with self.s.db() as db:db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',('data_link',json.dumps({'status':'中断'})))
  s=self.s.snapshot();self.assertEqual(s['tasks'][0]['level'],3);self.assertEqual(s['data_link']['status'],'中断');self.assertEqual(s['handoff'][0]['state'],'OPEN')
 def test_S5_ack_is_not_completion(self):
  t,e=self.create();self.s.reply({'task_id':t['id'],'actor':'primary','text':'已看到但还没处理'})
  t=self.s.snapshot()['tasks'][0];self.execute(self.decision(t,'wait',reply_status='ACKNOWLEDGED'),t)
  t=self.s.snapshot()['tasks'][0];self.assertEqual(t['state'],'ACKNOWLEDGED')
  with self.assertRaises(ValueError):self.s.feedback({'task_id':t['id'],'version':t['version'],'owner':'primary','state':'RESOLVED','note':'处理完毕'})
 def test_S5_explicit_completion_and_summary(self):
  t,e=self.create();self.s.end_episode(e['id'],'2026-11-19T13:00:20+00:00')
  r=self.s.feedback({'task_id':t['id'],'version':t['version'],'owner':'primary','state':'RESOLVED','note':'模拟查看完成','explicit_complete':True})
  h=self.s.snapshot()['handoff'][0];self.assertEqual(h['state'],'RESOLVED');self.assertTrue(h['resolved_at']);self.assertEqual(h['condition_duration_seconds'],23)
 def test_S6_observation_is_independent(self):
  t,e=self.create();self.s.reply({'task_id':t['id'],'actor':'primary','text':'可能误报，尚未查看','observation':'误报'})
  self.assertEqual(self.s.snapshot()['tasks'][0]['state'],'OPEN');self.assertEqual(self.s.snapshot()['feedback'][0]['observation'],'误报')
 def test_S7_notification_failure_visible(self):
  self.s.setting('simulate_notify_failure',True);t,e=self.create();self.assertEqual(self.s.snapshot()['receipts'][0]['status'],'failed');self.assertEqual(t['state'],'OPEN')
 def test_S7_invalid_policy_reference_and_action_rejected(self):
  for key,value in [('policy_version','fake'),('evidence_ids',['missing']),('action','resolve')]:
   t,e=self.create();self.execute(self.decision(t,**{key:value}),t);self.assertNotEqual(self.s.snapshot()['tasks'][0]['state'],'RESOLVED')
 def test_S7_three_model_failures_handoff(self):
  t,e=self.create()
  for _ in range(3):t=self.s.snapshot()['tasks'][0];self.execute(None,t)
  self.assertEqual(self.s.snapshot()['tasks'][0]['state'],'NEEDS_HUMAN')
 def test_S8_duplicate_restart_pause(self):
  t,e=self.create();self.s.ingest({'records':[{'kind':'event','data':e}]});self.assertEqual(len(self.s.snapshot()['tasks']),1);self.assertEqual(len(self.s.snapshot()['receipts']),1)
  self.s=PRDStore(self.s.path);self.assertEqual(len(self.s.snapshot()['tasks']),1)
  self.s.setting('paused',True);self.execute(self.decision(t),t);self.assertEqual(self.s.snapshot()['tasks'][0]['attempts'],0)
 def test_role_and_policy_approval_rollback(self):
  t,e=self.create()
  with self.assertRaises(ValueError):self.s.feedback({'task_id':t['id'],'version':1,'state':'RESOLVED','owner':'intruder','note':'done','explicit_complete':True})
  with self.assertRaises(ValueError):self.s.propose({'changes':{'medical_threshold':150}})
  p=self.s.propose({'changes':{'next_check_seconds':90},'reason':'减少非关键跟进打扰'})
  self.assertEqual(self.s.policy['next_check_seconds'],60)
  with self.assertRaises(ValueError):self.s.approve({'id':p['id'],'actor':'primary'})
  self.s.approve({'id':p['id'],'actor':'admin'});self.assertEqual(self.s.policy['next_check_seconds'],90)
  self.s.approve({'id':p['id'],'actor':'admin','rollback':True});self.assertEqual(self.s.policy['next_check_seconds'],60)

if __name__=='__main__':unittest.main(verbosity=2)

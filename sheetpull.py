import os, time, datetime, random, json, sqlite3, signal, uuid, requests, pydrive, flask, heroku3, boto
from time import sleep

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from threading import Lock
#Mutex lock used to prevent different workers getting the same batch id
assignBatchLock = Lock()

#AUTH to Google Drive
gauth = GoogleAuth()

gauth.LoadCredentialsFile("credentials.txt")
if gauth.credentials is None:
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    gauth.Refresh()
else:
    gauth.Authorize()

gauth.SaveCredentialsFile("credentials.txt")

drive = GoogleDrive(gauth)

#IAS3 Auth (Credentials provided via standard Boto Environment Variables)
from boto.s3.key import Key
from boto.s3.connection import OrdinaryCallingFormat

s3 = boto.connect_s3(host='s3.us.archive.org', is_secure=False, calling_format=OrdinaryCallingFormat())
S3_BUCKET = 'asdfjkljklsdfajkldsf'

sleep(15) #Safety cushion

#DL the DB
mysf = drive.CreateFile({'id': str(heroku3.from_key(os.environ['heroku-key']).apps()['getblogspot-01'].config()['dbid'])})
mysf.GetContentFile('db.db')
del mysf

from flask import Flask
from flask import Response
from flask import request
app = Flask(__name__)

#DB Inititalization
conn = sqlite3.connect('db.db')
conn.isolation_level= None # turn on autocommit to increase concurency
c = conn.cursor()

#Graceful Shutdown
class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True
        myul = drive.CreateFile({'title': 'db.db'})
        sleep(5)
        myul.SetContentFile('db.db')
        myul.Upload()
        heroku3.from_key(os.environ['heroku-key']).apps()['getblogspot-01'].config()['dbid'] = myul['id']
        del myul
        exit()

killer = GracefulKiller()

def getworkers(id):
    c.execute('select count(WorkerID ) from workers where WorkerID =?', (id,))
    return c.fetchone()[0]>0 

def addworker(ip):
    desid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(random.random())+str(random.random())+str(random.random())))#random.randint(1, 100000)#(myr[-1][0])+1
    c.execute('INSERT INTO "main"."workers"("WorkerID","CreatedTime","LastAliveTime","LastAliveIP") VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)', (desid,ip))
    complete = True
    return desid

def workeralive(id, ip):
    c.execute('UPDATE workers SET LastAliveTime=CURRENT_TIMESTAMP, LastAliveIP=? WHERE WorkerID=?', (ip, str(id),))
    return

def assignBatch(id, ip):
    randomkey = random.randint(1, 10000)
    #Mutex lock used to prevent different workers getting the same batch id
    with assignBatchLock:
                          #only one thread can execute here
        c.execute('SELECT BatchID from main where BatchStatus=0 LIMIT 1')
        ans = c.fetchall()[0][0]
        if not ans:
            return "Fail", "Fail"
        c.execute('UPDATE main SET BatchStatus=1, WorkerKey=?, RandomKey=?, AssignedTime=CURRENT_TIMESTAMP, BatchStatusUpdateTime=CURRENT_TIMESTAMP, BatchStatusUpdateIP=? WHERE BatchID=?',(id,randomkey,ip,ans))
    return ans, randomkey

def addtolist(list, id, batch, randomkey, item):
    item = item.lower()
    c.execute('SELECT '+str(list)+' FROM main WHERE BatchID=?', (str(batch),))
    res = c.fetchall()[0][0]
    splitter = ','
    if not res:
        splitter = ''
    if res:
        if str(item) in str(res).split(','):
            return 'Fail'
        splitter = str(res) + ','
    if list == 'Excluded':
        c.execute('INSERT into exclusions ("ExclusionName", "BatchStatus", "BatchStatusUpdateTime") VALUES (?, 0, CURRENT_TIMESTAMP)', (str(item),))
    c.execute('UPDATE main SET "'+str(list)+'"=? WHERE BatchID=?', ((str(splitter)+str(item)), str(batch)))
    return 'Success'

def updatestatus(id, batch, randomkey, status, ip):
    c.execute('SELECT BatchStatus from main where BatchID=?', (batch,))
    ans = c.fetchall()[0][0]
    if str(ans) != '1':
        return 'Fail'
    else:
        numstatus = ['f', '', 'c'].index(status)
        c.execute('UPDATE main SET BatchStatus=?, BatchStatusUpdateTime=CURRENT_TIMESTAMP, BatchStatusUpdateIP=? WHERE BatchID=? AND RandomKey=? AND WorkerKey=?', (numstatus, ip, batch, str(randomkey),str(id),))
        if status == 'f':
            return 'Success'
        elif status == 'c':
            return str(s3.generate_url(300, 'PUT', S3_BUCKET, str(batch)))

def verifylegitrequest(id, batch, randomkey, ip):
    c.execute('SELECT "_rowid_",* FROM main WHERE BatchStatus=1, WorkerKey=? AND BatchID=? AND RandomKey=?', (str(id),str(batch),str(randomkey)))
    res = bool(c.fetchall())
    if res:
        workeralive(id, ip)
    return res

def reopenavailability():
    c.execute("update main set BatchStatus=0,AssignedTime=null where BatchStatusUpdateTime< datetime('now', '-1 hour') and BatchStatus=1") #Thanks @jopik
    return 'Success'

@app.route('/worker/getID')
def give_id():
    ip = request.headers['X-Forwarded-For']
    return str(addworker(ip))

@app.route('/worker/getBatch') #Parameters: id
def give_batch():
    id = str(request.args.get('id', ''))
    ip = request.headers['X-Forwarded-For']
    workeralive(id, ip)
    if not getworkers(id):
        return 'Fail'
    batchid, randomkey = assignBatch(id, ip)
    myj = {'batchID': batchid, 'randomKey': str(randomkey)}
    myresp = Response(json.dumps(myj), mimetype='application/json')
    return myresp

@app.route('/worker/submitExclusion')
def submit_exclusion(): #Parameters: id, batchID, randomKey, exclusion
    id = request.args.get('id', '')
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    target = request.args.get('exclusion', '')
    ip = request.headers['X-Forwarded-For']
    if not verifylegitrequest(id, batchid, randomkey, ip):
        return 'Fail'
    if not target:
        return 'Fail'
    return(addtolist("Excluded", id, batchid, randomkey, target))

@app.route('/worker/submitDeleted')
def submit_deleted(): #Parameters: id, batchID, randomKey, deleted
    id = request.args.get('id', '')
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    target = request.args.get('deleted', '')
    ip = request.headers['X-Forwarded-For']
    if not verifylegitrequest(id, batchid, randomkey, ip):
        return 'Fail'
    if not target:
        return 'Fail'
    return(addtolist("Deleted", id, batchid, randomkey, target))

@app.route('/worker/submitPrivate')
def submit_private(): #Parameters: id, batchID, randomKey, private
    id = request.args.get('id', '')
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    target = request.args.get('private', '')
    ip = request.headers['X-Forwarded-For']
    if not verifylegitrequest(id, batchid, randomkey, ip):
        return 'Fail'
    if not target:
        return 'Fail'
    return(addtolist("Privated", id, batchid, randomkey, target))

@app.route('/worker/updateStatus')
def update_status(): #Parameters: id, batchID, randomKey, status ('a'=assigned,) 'c'=completed, 'f'=failed
    id = request.args.get('id', '')
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    status = request.args.get('status', '')
    ip = request.headers['X-Forwarded-For']
    if not verifylegitrequest(id, batchid, randomkey, ip):
        return 'Fail'
    if not status in ['c', 'f']: #valid submission
        return 'Fail'
    else:
        return updatestatus(id, batchid, randomkey, status, ip)

@app.route('/internal/dumpdb')
def dumpdb():
    myul = drive.CreateFile({'title': 'dbDUMP.db'})
    myul.SetContentFile('db.db')
    myul.Upload()
    return str(myul['id'])

@app.route('/internal/purgeinactive')
def request_reopen():
    return reopenavailability()

@app.route('/internal/ipcheck')
def give_ip():
    return request.headers['X-Forwarded-For']

@app.route('/robots.txt')
def download_robots_txt():
    return Response('User-agent: *\nDisallow: /', mimetype='text/plain')

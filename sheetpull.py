import os, time, datetime, random, json, sqlite3, signal, uuid, requests, pydrive, flask, heroku3
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

sleep(20) #Safety cushion

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

def addworker():
    desid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(random.random())+str(random.random())+str(random.random())))#random.randint(1, 100000)#(myr[-1][0])+1
    c.execute('INSERT INTO "main"."workers"("WorkerID","CreatedTime","LastAliveTime") VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)', (desid,))
    complete = True
    return desid

def workeralive(id):
    c.execute('UPDATE workers SET LastAliveTime=CURRENT_TIMESTAMP WHERE WorkerID=?', (str(id),))
    return

def assignBatch(id):
    randomkey = random.randint(1, 10000)
    #Mutex lock used to prevent different workers getting the same batch id
    with assignBatchLock:
                          #only one thread can execute here
        c.execute('SELECT BatchID from main where BatchStatus=0 LIMIT 1')
        ans = c.fetchall()[0][0]
        if not ans:
            return "Fail", "Fail"
        c.execute('UPDATE main SET BatchStatus=1, WorkerKey=?, RandomKey=?, AssignedTime=CURRENT_TIMESTAMP, BatchStatusUpdateTime=CURRENT_TIMESTAMP WHERE BatchID=?',(id,randomkey,ans,))
    return ans, randomkey

def addtolist(list, id, batch, randomkey, item):
    #try:
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
    #except:
    #    return 'Fail'

def updatestatus(id, batch, randomkey, status):
    #try:
        numstatus = ['f', '', 'c'].index(status)
        c.execute('UPDATE main SET BatchStatus=?, BatchStatusUpdateTime=CURRENT_TIMESTAMP WHERE BatchID=? AND RandomKey=? AND WorkerKey=?', (numstatus, str(batch),str(randomkey),str(id),))
        return 'Success'
    #except:
    #    return 'Fail'

def verifylegitrequest(id, batch, randomkey):
    #try:
        c.execute('SELECT "_rowid_",* FROM main WHERE WorkerKey=? AND BatchID=? AND RandomKey=?', (str(id),str(batch),str(randomkey)))
        res = bool(c.fetchall())
        if res:
            workeralive(id)
        return res
    #except:
    #    return False

def reopenavailability():
    c.execute("update main set BatchStatus=0,AssignedTime=null where BatchStatusUpdateTime< datetime('now', '-1 hour') and BatchStatus=1") #Thanks @jopik
    return 'Success'

@app.route('/worker/getID')
def give_id():
    return str(addworker())

@app.route('/worker/getBatch') #Parameters: id
def give_batch():
    id = str(request.args.get('id', ''))
    workeralive(id)
    if not getworkers(id):
        return 'Fail'
    batchid, randomkey = assignBatch(id)
    myj = {'batchID': batchid, 'randomKey': str(randomkey)}
    myresp = Response(json.dumps(myj), mimetype='application/json')
    return myresp

@app.route('/worker/submitExclusion')
def submit_exclusion(): #Parameters: id, batchID, randomKey, exclusion
    id = request.args.get('id', '')
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    target = request.args.get('exclusion', '')
    if not verifylegitrequest(id, batchid, randomkey):
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
    if not verifylegitrequest(id, batchid, randomkey):
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
    if not verifylegitrequest(id, batchid, randomkey):
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
    if not verifylegitrequest(id, batchid, randomkey):
        return 'Fail'
    if not status in ['c', 'f']: #valid submission
        return 'Fail'
    else:
        updatestatus(id, batchid, randomkey, status)
        return 'Success'

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

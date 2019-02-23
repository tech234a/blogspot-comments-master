import os, time, datetime, random, json, sqlite3, signal, uuid, csv, requests, pydrive, flask, heroku3
from time import sleep

from hurry.filesize import size, alternative

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

infile = open('offset.csv', mode='r')
reader = csv.reader(infile)
offsets = dict((rows[0],rows[1]) for rows in reader)
infile.close()

mysf = drive.CreateFile({'id': '1U27xcHSk91JXj2-1TVvoVpBJq3HQDLvt'})
mysf.GetContentFile('domains_list.txt')

sleep(10) #Safety cushion

#DL the DB
mysf = drive.CreateFile({'id': str(heroku3.from_key(os.environ['heroku-key']).apps()['getblogspot-01'].config()['dbid'])})
mysf.GetContentFile('db.db')
del mysf

from flask import Flask
from flask import Response
from flask import request
from flask_caching import Cache
app = Flask(__name__)
cache = Cache(app,config={'CACHE_TYPE': 'simple'})

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
    limit = 450
    randomkey = random.randint(1, 10000)
    #Mutex lock used to prevent different workers getting the same batch id
    with assignBatchLock:
                          #only one thread can execute here
        c.execute('SELECT * from main where BatchStatus=0 LIMIT 1')
        datalist = c.fetchone()
        ans = datalist[0]
        if datalist[4]:
            dltype = "domain"
            content = datalist[4]
        else:
            dltype = "list"
            content = ""
        myoffset = offsets[str(ans)]
        if not ans:
            return "Fail", "Fail"
        c.execute('UPDATE main SET BatchStatus=1, WorkerKey=?, RandomKey=?, AssignedTime=CURRENT_TIMESTAMP, BatchStatusUpdateTime=CURRENT_TIMESTAMP, BatchStatusUpdateIP=? WHERE BatchID=?',(id,randomkey,ip,ans))
    return ans, randomkey, myoffset, limit, dltype, content

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
        c.execute('INSERT into main (BatchContent, BatchStatus) VALUES(?,0)', (str(item),))
        #c.execute('INSERT into exclusions ("ExclusionName", "BatchStatus", "BatchStatusUpdateTime") VALUES (?, 0, CURRENT_TIMESTAMP)', (str(item),))
    c.execute('UPDATE main SET "'+str(list)+'"=? WHERE BatchID=?', ((str(splitter)+str(item)), str(batch)))
    return 'Success'

def updatestatus(id, batch, randomkey, status, ip):
    c.execute('SELECT BatchStatus from main where BatchID=?', (batch,))
    ans = c.fetchall()[0][0]
    if str(ans) != '1':
        return 'Fail'
    else:
        numstatus = ['f', '', 'c'].index(status)
        if status == 'c':
            myrdata = requests.get('http://blogstore.bot.nu/getVerifyBatchUnit?batchID='+str(batch)+'&batchKey='+str(randomkey))
            if myrdata.status_code != 200:
                return 'Fail'
            size = int(myrdata.json['size'])
            c.execute('UPDATE main SET BatchStatus=?, BatchStatusUpdateTime=CURRENT_TIMESTAMP, BatchStatusUpdateIP=?, BatchSize=? WHERE BatchID=? AND RandomKey=? AND WorkerKey=?', (numstatus, ip, size, batch, str(randomkey), str(id),))
            return 'Success'
        c.execute('UPDATE main SET BatchStatus=?, BatchStatusUpdateTime=CURRENT_TIMESTAMP, BatchStatusUpdateIP=? WHERE BatchID=? AND RandomKey=? AND WorkerKey=?', (numstatus, ip, batch, str(randomkey),str(id),))
        return 'Success'

def verifylegitrequest(id, batch, randomkey, ip):
    c.execute('SELECT * FROM main WHERE BatchStatus=1 AND WorkerKey=? AND BatchID=? AND RandomKey=?', (str(id),str(batch),str(randomkey),))
    res = bool(c.fetchall())
    if res:
        workeralive(id, ip)
    return res

def reopenavailability():
    c.execute("update main set BatchStatus=0,AssignedTime=null where BatchStatusUpdateTime< datetime('now', '-1 hour') and BatchStatus=1") #Thanks @jopik
    return 'Success'

def gen_stats():
    result = {}
    c.execute('SELECT count(*) FROM main WHERE BatchStatus=1')
    result['batches_assigned'] = c.fetchone()[0]
    c.execute('SELECT count(*) FROM main WHERE BatchStatus=2')
    result['batches_completed'] = c.fetchone()[0]
    c.execute("SELECT count(*) FROM main WHERE BatchStatusUpdateTime> datetime('now', '-1 hour') and BatchStatus=2")
    result['batches_completed_last_hour'] = c.fetchone()[0]
    c.execute('SELECT sum(BatchSize) FROM main')
    c.execute('SELECT count(*) FROM main WHERE BatchStatus=0')
    result['batches_remaining'] = c.fetchone()[0]
    try:
        result['total_data_size'] = c.fetchone()[0]
    except:
        result['total_data_size'] = 0
    result['total_data_size_pretty'] = size(result['total_data_size'], system=alternative)
    c.execute('SELECT count(BatchContent) FROM main')
    result['total_exclusions'] = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM workers') 
    result['worker_count'] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM workers where LastAliveTime> datetime('now', '-1 hour')")
    result['worker_count_last_hour'] = c.fetchone()[0]
    return json.dumps(result)
    
    
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
    batchid, randomkey, curroffset, limit, dltype, content = assignBatch(id, ip)
    myj = {'batchID': batchid, 'randomKey': str(randomkey), 'offset': curroffset, 'limit': limit, 'assignmentType': dltype, 'content': content}
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
    
@app.route('/worker/getStats')
@cache.cached(timeout=30)
def get_stats():
    return Response(gen_stats(), mimetype='application/json')

@app.route('/worker/domains.txt')
def download_list():
    return flask.send_file("domains_list.txt", mimetype='text/plain', as_attachment=True)

@app.route('/internal/dumpdb')
def dumpdb():
    myul = drive.CreateFile({'title': 'dbDUMP.db'})
    myul.SetContentFile('db.db')
    myul.Upload()
    return str(myul['id'])

@app.route('/internal/purgeinactive')
def request_reopen():
    return reopenavailability()

@app.route('/robots.txt')
def download_robots_txt():
    return Response('User-agent: *\nDisallow: /', mimetype='text/plain')

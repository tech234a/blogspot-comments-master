import os, time, datetime, random, json, sqlite3, signal, uuid, csv, requests, pydrive, flask, fasteners
from time import sleep

from hurry.filesize import size, alternative

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from multiprocessing import Lock
from threading import Lock
#Mutex lock used to prevent different workers getting the same batch id
assignBatchLock = Lock()

drive_lock = fasteners.InterProcessLock('/dev/shm/drive_lock_file')
print('Requesting lock')
gotten = drive_lock.acquire(blocking=True)
print('Got lock')

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

drive_lock.release()

#infile = open('offset.csv', mode='r')
#reader = csv.reader(infile)
#offsets = dict((rows[0],rows[1]) for rows in reader)
#infile.close()

#mysf = drive.CreateFile({'id': '[id of file to be used]'})
#mysf.GetContentFile('domains_list.txt')

#sleep(15) #Safety cushion

#a_lock = fasteners.InterProcessLock('tmp_lock_file_init')
#gotten = a_lock.acquire(blocking=False)
#if gotten and (not os.path.exists('db.db')):
#    print('Gotten lock')
    #DL the DB
#    mysf = drive.CreateFile({'id': str(heroku3.from_key(os.environ['heroku-key']).apps()['getblogspot-01'].config()['dbid'])})
#    mysf.GetContentFile('db.db')
#    del mysf
#    a_lock.release()
#    print('Loaded DB')
#else:
#    sleep(7)
#    print('T2 up')
    #print('Waiting for lock to finish...')
    #gotten = a_lock.acquire()
    #print('DB Done')


from flask import Flask
from flask import Response
from flask import request
from flask_caching import Cache
app = Flask(__name__)
cache = Cache(app,config={'CACHE_TYPE': 'simple'})

#DB Inititalization
conn = sqlite3.connect('/dev/shm/dbfile.db')
conn.isolation_level= None # turn on autocommit to increase concurency
c = conn.cursor()

#def progress(status, remaining, total):
#    print(f'Copied {total-remaining} of {total} pages...')

#Graceful Shutdown
class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True
        sleep(3)
        #if os.path.exists("backup_quit.db"):
        #  os.remove("backup_quit.db")
        #else:
        #  print("The file does not exist")
        a_lock = fasteners.InterProcessLock('tmp_lock_file')
        gotten = a_lock.acquire(blocking=False)
        if gotten:
            print('Gotten lock')
            with sqlite3.connect('backup_quit.db') as bck:
                conn.backup(bck)#, pages=1, progress=progress)
            #myul = drive.CreateFile({'title': 'db.db'})
            #myul.SetContentFile('backup_quit.db')
            #myul.Upload()
            #heroku3.from_key(os.environ['heroku-key']).apps()['getblogspot-01'].config()['dbid'] = myul['id']
            #del myul
            a_lock.release()
        print('Exiting...')    
        exit()

killer = GracefulKiller()

def getworkers(id):
    c.execute('select count(WorkerID ) from workers where WorkerID =?', (id,))
    return c.fetchone()[0]>0 

def addworker(ip, ver):
    if ver == '':
	    ver = 0
    desid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(random.random())+str(random.random())+str(random.random())))#random.randint(1, 100000)#(myr[-1][0])+1
    c.execute('INSERT INTO "main"."workers"("WorkerID","CreatedTime","LastAliveTime","LastAliveIP","WorkerVersion") VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?)', (desid,ip,ver,))
    complete = True
    return desid

def workeralive(id, ip):
    c.execute('UPDATE workers SET LastAliveTime=CURRENT_TIMESTAMP, LastAliveIP=? WHERE WorkerID=?', (ip, str(id),))
    return

def assignBatch(id, ip, ver):
    if ver == '':
	    ver = 0
    limit = 300#100#80#250#450
    batchsize = 250
    batch_lock = fasteners.InterProcessLock('/dev/shm/batchassign_lock_file')
    print('Requesting lock')
    gotten = batch_lock.acquire(blocking=True)
    print('Got lock')
    try:
        #reopenavailability()
        #Mutex lock used to prevent different workers getting the same batch id
        with assignBatchLock:
                              #only one thread can execute here
            if int(ver) != 0: #newer versions
                c.execute('SELECT BatchID, BatchContent, RandomKey from main where BatchStatus=0 AND BatchContent IS NULL LIMIT 1')
                datalist = c.fetchone()
                if datalist == None:
                    c.execute('SELECT BatchID, BatchContent, RandomKey from main where BatchStatus=0 LIMIT 1')
                    datalist = c.fetchone()
            else: #old versions, exclusions only
                c.execute('SELECT BatchID, BatchContent, RandomKey from main where BatchStatus=0 AND BatchContent IS NOT NULL LIMIT 1')
                datalist = c.fetchone()
            if datalist == None:
                print('Releasing')
                batch_lock.release()
                return "Fail", "Fail", "Fail", "Fail", "Fail", "Fail", "Fail"
            ans = datalist[0]
            if datalist[2]:
                randomkey = datalist[2]
            else:
                randomkey = '2-'+str(random.randint(1, 10000000))
            c.execute('UPDATE main SET BatchStatus=1, WorkerKey=?, RandomKey=?, AssignedTime=CURRENT_TIMESTAMP, BatchStatusUpdateTime=CURRENT_TIMESTAMP, BatchStatusUpdateIP=? WHERE BatchID=?',(id,randomkey,ip,ans))
            print('Releasing')
            batch_lock.release()
            myoffset=0
            if datalist[1]:
                dltype = "domain"
                content = datalist[1]
                limit = 0
                batchsize = 1
            else:
                dltype = "list"
                c.execute('SELECT BatchContent from batches where BatchID=?', (ans,))
                content = str(c.fetchone()[0]).replace('\n', '')
                myoffset = 0#offsets[str(ans)]
        return ans, randomkey, myoffset, limit, dltype, content, batchsize
    except:
        print('Releasing')
        batch_lock.release()
        raise
        
def addtolist(list, id, batch, randomkey, item):
    item = item.lower()
    c.execute('SELECT '+str(list)+' FROM main WHERE BatchID=?', (str(batch),))
    res = c.fetchall()[0][0]
    splitter = ','
    if not res:
        splitter = ''
    if res:
        if str(item) in str(res).split(','):
            return 'Dupe'
        splitter = str(res) + ','
    if list == 'Excluded':
        c.execute('SELECT COUNT(*) from main where BatchContent=?', (str(item),))
        if bool(c.fetchone()[0]):
            return 'Dupe'
        c.execute('INSERT into main (BatchContent, BatchStatus) VALUES(?,0)', (str(item),))
        #c.execute('INSERT into exclusions ("ExclusionName", "BatchStatus", "BatchStatusUpdateTime") VALUES (?, 0, CURRENT_TIMESTAMP)', (str(item),))
    c.execute('UPDATE main SET "'+str(list)+'"=? WHERE BatchID=?', ((str(splitter)+str(item)), str(batch)))
    return 'Success'

def addcustom(item, domain):
    c.execute('SELECT COUNT(*) from domains where blogspot=?', (str(item),))
    if bool(c.fetchone()[0]):
        return 'Dupe'
    c.execute('SELECT COUNT(*) from domains where custom=?', (str(domain),))
    if bool(c.fetchone()[0]):
        return 'Dupe'
    c.execute('INSERT into domains (blogspot, custom) VALUES(?,?)', (str(item),str(domain),))
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
            size = int(myrdata.json()['size'])
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

@cache.cached(timeout=300, key_prefix='purge_inactive_tasks')
def reopenavailability():
    c.execute("update main set BatchStatus=0,AssignedTime=null where BatchStatusUpdateTime< datetime('now', '-2 hour') and BatchStatus=1") #Thanks @jopik
    return 'Success'

def gen_stats():
    result = {}
    c.execute("select avg(strftime('%s',BatchStatusUpdateTime) -strftime('%s',AssignedTime) ) from main where BatchStatus=2")
    result['average_batch_time_seconds'] = c.fetchone()[0]
    c.execute("select avg(strftime('%s',BatchStatusUpdateTime) -strftime('%s',AssignedTime) ) from main where BatchStatus=2 AND BatchContent IS NULL")
    result['average_nonexclusion_batch_time_seconds'] = c.fetchone()[0]
    c.execute("select avg(strftime('%s',BatchStatusUpdateTime) -strftime('%s',AssignedTime) ) from main where BatchStatus=2 AND BatchContent IS NOT NULL")
    result['average_exclusion_batch_time_seconds'] = c.fetchone()[0]
    
    c.execute('SELECT count(*) FROM main WHERE BatchStatus=1')
    result['batches_assigned'] = c.fetchone()[0]
    c.execute('SELECT count(*) FROM main WHERE BatchStatus=2')
    result['batches_completed'] = c.fetchone()[0]
    c.execute("SELECT count(*) FROM main WHERE BatchStatusUpdateTime> datetime('now', '-10 minute') and BatchStatus=2")
    result['batches_completed_last_10_minutes'] = c.fetchone()[0]
    c.execute("SELECT count(*) FROM main WHERE BatchStatusUpdateTime> datetime('now', '-1 hour') and BatchStatus=2")
    result['batches_completed_last_hour'] = c.fetchone()[0]
    
    c.execute('SELECT count(*) FROM main WHERE BatchContent IS NULL AND BatchStatus=1')
    result['nonexclusion_batches_assigned'] = c.fetchone()[0]
    c.execute('SELECT count(*) FROM main WHERE BatchContent IS NULL AND BatchStatus=2')
    result['nonexclusion_batches_completed'] = c.fetchone()[0]
    c.execute("SELECT count(*) FROM main WHERE BatchContent IS NULL AND BatchStatusUpdateTime> datetime('now', '-10 minute') and BatchStatus=2")
    result['nonexclusion_batches_completed_last_10_minutes'] = c.fetchone()[0]
    c.execute("SELECT count(*) FROM main WHERE BatchContent IS NULL AND BatchStatusUpdateTime> datetime('now', '-1 hour') and BatchStatus=2")
    result['nonexclusion_batches_completed_last_hour'] = c.fetchone()[0]
    c.execute('SELECT count(*) FROM main WHERE BatchContent IS NULL AND (BatchStatus=0 OR BatchStatus=1)')
    result['nonexclusion_batches_remaining'] = c.fetchone()[0]
    c.execute('SELECT count(*) FROM main WHERE BatchContent IS NULL')
    result['nonexclusion_batches_total'] = c.fetchone()[0]
    result['nonexclusion_batches_completed_percent'] = (result['nonexclusion_batches_completed']/(result['nonexclusion_batches_total']))*100
    try:
        result['nonexclusion_projected_hours_remaining_10_min_base'] = (result['nonexclusion_batches_remaining'])/(result['nonexclusion_batches_completed_last_10_minutes']*6)
    except ZeroDivisionError:
        result['nonexclusion_projected_hours_remaining_10_min_base'] = None
    try:
        result['nonexclusion_projected_hours_remaining_1_hour_base'] = (result['nonexclusion_batches_remaining'])/(result['nonexclusion_batches_completed_last_hour'])
        result['nonexclusion_projected_hours_remaining'] = result['nonexclusion_projected_hours_remaining_1_hour_base'] #(result['average_batch_time_seconds'] * (result['nonexclusion_batches_remaining']-(0.9*result['total_exclusions'])))/3600
    except ZeroDivisionError:
        result['nonexclusion_projected_hours_remaining_1_hour_base'] = None
        result['nonexclusion_projected_hours_remaining'] = None
    
    c.execute("SELECT count(BatchContent) FROM main WHERE BatchStatusUpdateTime> datetime('now', '-10 minute') and BatchStatus=2")
    result['exclusions_completed_last_10_minutes'] = c.fetchone()[0]
    c.execute("SELECT count(BatchContent) FROM main WHERE BatchStatusUpdateTime> datetime('now', '-1 hour') and BatchStatus=2")
    result['exclusions_completed_last_hour'] = c.fetchone()[0]
    c.execute('SELECT count(*) FROM main WHERE (BatchStatus=0 OR BatchStatus=1)')
    result['batches_remaining'] = c.fetchone()[0]
    c.execute('SELECT count(*) FROM main')
    result['batches_total'] = c.fetchone()[0]
    c.execute('SELECT COUNT(*) from domains')
    result['total_custom_domains'] = c.fetchone()[0]
    c.execute('SELECT sum(BatchSize) FROM main')
    try:
        result['total_data_size'] = c.fetchone()[0]
    except:
        result['total_data_size'] = 0
    if result['total_data_size'] == None:
        result['total_data_size'] = 0
    result['total_data_size_pretty'] = size(result['total_data_size'], system=alternative)
    c.execute('SELECT count(BatchContent) FROM main')
    result['total_exclusions'] = c.fetchone()[0]
    c.execute('SELECT count(BatchContent) FROM main where BatchStatus=0')
    result['exclusions_unassigned'] = c.fetchone()[0]
    
    c.execute('SELECT count(BatchContent) FROM main where BatchStatus=2')
    result['exclusions_completed'] = c.fetchone()[0]
    
    try:
        result['exclusion_projected_hours_remaining_10_min_base'] = (result['exclusions_unassigned'])/(result['exclusions_completed_last_10_minutes']*6)
    except ZeroDivisionError:
        result['exclusion_projected_hours_remaining_10_min_base'] = None
    try:
        result['exclusion_projected_hours_remaining_1_hour_base'] = (result['exclusions_unassigned'])/(result['exclusions_completed_last_hour'])
        result['exclusion_projected_hours_remaining'] = result['exclusion_projected_hours_remaining_1_hour_base'] #(result['average_batch_time_seconds'] * (result['exclusion_batches_remaining']-(0.9*result['total_exclusions'])))/3600
    except ZeroDivisionError:
        result['exclusion_projected_hours_remaining_1_hour_base'] = None
        result['exclusion_projected_hours_remaining'] = None
    
    result['exclusion_batches_completed_percent'] = (result['exclusions_completed']/(result['total_exclusions']))*100
    
    result['batches_completed_percent'] = (result['batches_completed']/(result['batches_total']))*100 #-(0.9*result['total_exclusions'])))*100
    
    # try:
        # result['projected_hours_remaining_10_min_base'] = 
    # except:
        # result['projected_hours_remaining_10_min_base'] = None
    # try:
        # result['projected_hours_remaining_1_hour_base'] = 
        # result['projected_hours_remaining'] = result['projected_hours_remaining_1_hour_base'] #(result['average_batch_time_seconds'] * (result['batches_remaining']-(0.9*result['total_exclusions'])))/3600
    # except:
        # result['projected_hours_remaining_1_hour_base'] = None
        # result['projected_hours_remaining'] = None
        
    try:
        result['projected_hours_remaining_10_min_base'] = (result['batches_remaining'])/(result['batches_completed_last_10_minutes']*6)
    except ZeroDivisionError:
        result['projected_hours_remaining_10_min_base'] = None
    try:
        result['projected_hours_remaining_1_hour_base'] = (result['batches_remaining'])/(result['batches_completed_last_hour'])
        #result['projected_hours_remaining'] = result['projected_hours_remaining_1_hour_base'] #(result['average_batch_time_seconds'] * (result['batches_remaining']-(0.9*result['total_exclusions'])))/3600
    except ZeroDivisionError:
        result['projected_hours_remaining_1_hour_base'] = None
        #result['projected_hours_remaining'] = None
    
    result['projected_hours_remaining'] = ((result['average_exclusion_batch_time_seconds'] * result['exclusions_unassigned']) + (result['average_nonexclusion_batch_time_seconds'] * result['nonexclusion_batches_remaining']))/3600
    
    c.execute('SELECT COUNT(*) FROM workers') 
    result['worker_count'] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM workers where LastAliveTime> datetime('now', '-10 minute')")
    result['worker_count_last_10_minutes'] = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM workers where LastAliveTime> datetime('now', '-1 hour')")
    result['worker_count_last_hour'] = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM workers where LastAliveTime> datetime('now', '-1 hour') AND WorkerVersion=4")
    result['version_4_workers_last_hour'] = c.fetchone()[0]
    result['percent_version_4_workers_last_hour'] = (result['version_4_workers_last_hour']/result['worker_count_last_hour'])*100
    
    c.execute('SELECT COUNT(DISTINCT LastAliveIP) FROM workers') 
    result['worker_ip_count'] = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT LastAliveIP) FROM workers where LastAliveTime> datetime('now', '-10 minute')")
    result['worker_ip_count_last_10_minutes'] = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT LastAliveIP) FROM workers where LastAliveTime> datetime('now', '-1 hour')")
    result['worker_ip_count_last_hour'] = c.fetchone()[0]
    return json.dumps(result)
    
    
@app.route('/worker/getID')
def give_id():
    ip = request.remote_addr
    ver = str(request.args.get('worker_version', ''))
    return str(addworker(ip, ver))

@app.route('/worker/getBatch') #Parameters: id
def give_batch():
    id = str(request.args.get('id', ''))
    ver = str(request.args.get('worker_version', ''))
    #print(ver)
    ip = request.remote_addr
    workeralive(id, ip)
    if not getworkers(id):
        return 'Fail'
    batchid, randomkey, curroffset, limit, dltype, content, batchsize = assignBatch(id, ip, ver)
    myj = {'batchID': batchid, 'randomKey': str(randomkey), 'offset': curroffset, 'limit': limit, 'assignmentType': dltype, 'content': content, 'batchSize': batchsize}
    myresp = Response(json.dumps(myj), mimetype='application/json')
    return myresp

@app.route('/worker/submitExclusion')
def submit_exclusion(): #Parameters: id, batchID, randomKey, exclusion
    id = request.args.get('id', '')
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    target = request.args.get('exclusion', '')
    ip = request.remote_addr
    if not verifylegitrequest(id, batchid, randomkey, ip):
        return 'Fail'
    if not target:
        return 'Fail'
    return(addtolist("Excluded", id, batchid, randomkey, target))

@app.route('/worker/submitDeleted')
def submit_deleted(): #Parameters: id, batchID, randomKey, deleted
    return 'Success'
    # id = request.args.get('id', '')
    # batchid = request.args.get('batchID', '')
    # randomkey = request.args.get('randomKey', '')
    # target = request.args.get('deleted', '')
    # ip = request.remote_addr
    # if not verifylegitrequest(id, batchid, randomkey, ip):
        # return 'Fail'
    # if not target:
        # return 'Fail'
    # return(addtolist("Deleted", id, batchid, randomkey, target))

@app.route('/worker/submitPrivate')
def submit_private(): #Parameters: id, batchID, randomKey, private
    return 'Success'
    # id = request.args.get('id', '')
    # batchid = request.args.get('batchID', '')
    # randomkey = request.args.get('randomKey', '')
    # target = request.args.get('private', '')
    # ip = request.remote_addr
    # if not verifylegitrequest(id, batchid, randomkey, ip):
        # return 'Fail'
    # if not target:
        # return 'Fail'
    # return(addtolist("Privated", id, batchid, randomkey, target))

@app.route('/worker/submitDomain')
def submit_domain(): #Parameters: id, batchID, randomKey, blog, domain
    id = request.args.get('id', '')
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    target = request.args.get('blog', '')
    customdomain = request.args.get('domain', '')
    ip = request.remote_addr
    if not verifylegitrequest(id, batchid, randomkey, ip):
        return 'Fail'
    if not target:
        return 'Fail'
    if not customdomain:
        return 'Fail'
    return(addcustom(target, customdomain))

@app.route('/worker/updateStatus')
def update_status(): #Parameters: id, batchID, randomKey, status ('a'=assigned,) 'c'=completed, 'f'=failed
    id = request.args.get('id', '')
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    status = request.args.get('status', '')
    ip = request.remote_addr
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

@app.route('/worker/domains.txt.gz')
def download_list_gz():
    return flask.send_file("domains_list.txt.gz", mimetype='text/plain', as_attachment=True)

@app.route('/internal/dumpdb')
@cache.cached(timeout=300)
def dumpdb():
    #if os.path.exists("backup.db"):
    #    os.remove("backup.db")
    #else:
    #    print("The file does not exist")
    with sqlite3.connect('backup.db') as bck:
        conn.backup(bck, pages=1)#, pages=1, progress=progress)
    myul = drive.CreateFile({'title': 'dbDUMP.db'})
    myul.SetContentFile('backup.db')
    myul.Upload()
    return str(myul['id'])

@app.route('/wakemydyno.txt')
def wake_registration():
    return Response('OK', mimetype='text/plain')

@app.route('/internal/purgeinactive')
def request_reopen():
    return reopenavailability()

@app.route('/robots.txt')
def download_robots_txt():
    return Response('User-agent: *\nDisallow: /', mimetype='text/plain')

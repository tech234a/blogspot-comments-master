import pickle, os, re, time, datetime, random, json, threading, requests, pydrive
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from time import sleep
from hurry.filesize import size, alternative

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

import flask
from flask import Flask
from flask import Response
from flask import request
app = Flask(__name__)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = None
# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
        creds = flow.run_local_server()
    # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

service = build('sheets', 'v4', credentials=creds)

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

MEM_SHEET_ID = os.environ['memsheetid']
WORKER_SHEET_ID = os.environ['workersheetid']

def cooldown():
    return random.randint(100, 150)

def getcell(cellloc):

        # The ID and range of a sample spreadsheet.
    #spreadsheet_id = MEM_SHEET_ID
    #range_name = 'Sheet1!A1:A'

    

    # Call the Sheets API
    # The ID of the spreadsheet to update.
    spreadsheet_id = MEM_SHEET_ID  # TODO: Update placeholder value.

    # The A1 notation of the values to update.
    range_ = 'Sheet1!'+cellloc  # TODO: Update placeholder value.

    # How the input data should be interpreted.
    value_render_option = 'RAW'  # TODO: Update placeholder value.

    request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_) #, valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option)
    complete = False
    while not complete:
        try:
            response = request.execute()
            complete = True
        except:
            sleep(cooldown())
    #print(response)
    try:
        finalresp = response['values'][0][0]
    except:
        finalresp = ''
    return finalresp

def getworkers():

        # The ID and range of a sample spreadsheet.
    #spreadsheet_id = WORKER_SHEET_ID
    #range_name = 'Sheet1!A1:A'


    

    # Call the Sheets API
    # The ID of the spreadsheet to update.
    spreadsheet_id = WORKER_SHEET_ID  # TODO: Update placeholder value.

    # The A1 notation of the values to update.
    range_ = 'Sheet1!A:A'  # TODO: Update placeholder value.

    # How the input data should be interpreted.
    value_render_option = 'RAW'  # TODO: Update placeholder value.

    request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_, majorDimension='COLUMNS') #, valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option, majorDimension='COLUMNS')
    complete = False
    while not complete:
        try:
            response = request.execute()
            complete = True
        except:
            sleep(cooldown())
    #print(response)
    try:
        finalresp = response['values'][0]
    except:
        finalresp = ''
    return finalresp

def addworker(id):
    #mylist = [str(datetime.datetime.now()).split('.')[0]]
    #mylist.extend(inlist)   
    
    # The ID and range of a sample spreadsheet.
    spreadsheet_id = WORKER_SHEET_ID
    range_name = 'Sheet1!A:A'
    
    # Call the Sheets API
    values = [[id, str(datetime.datetime.now()).split('.')[0], str(datetime.datetime.now()).split('.')[0]]]
    body = {
            'values': values
    }
    request = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption="RAW", body=body)
    #print(result['tableRange'])
    
    complete = False
    while not complete:
        try:
            result = request.execute()
            complete = True
        except:
            sleep(cooldown())
    print('{0} cells appended.'.format(result \
                                                                               .get('updates') \
                                                                               .get('updatedCells')))
    return result['updates']['updatedRange'].split(':')[-1][1:]

def workeralive(id):
    
    # The ID and range of a sample spreadsheet.
    #spreadsheet_id = MEM_SHEET_ID
    #range_name = 'Sheet1!A1:A'

    # Call the Sheets API
    # The ID of the spreadsheet to update.
    spreadsheet_id = WORKER_SHEET_ID  # TODO: Update placeholder value.
    
    try:
        cellloc = 'C'+str(getworkers().index(id)+1)
    except:
        sleep(5)
        cellloc = 'C'+str(getworkers().index(id)+1)
    # The A1 notation of the values to update.
    range_ = 'Sheet1!'+cellloc+':'+cellloc  # TODO: Update placeholder value.

    # How the input data should be interpreted.
    value_input_option = 'RAW'  # TODO: Update placeholder value.
    
    value = str(datetime.datetime.now()).split('.')[0]
    value_range_body = {
            'values': [[str(value)]]
    }

    request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, body=value_range_body)
    complete = False
    while not complete:
        try:
            response = request.execute()
            complete = True
        except:
            sleep(cooldown())

def listexclusions(cellloc):

    # Call the Sheets API
    # The ID of the spreadsheet to update.
    spreadsheet_id = MEM_SHEET_ID  # TODO: Update placeholder value.

    # The A1 notation of the values to update.
    range_ = 'Sheet1!C:C' #+cellloc  # TODO: Update placeholder value.

    # How the input data should be interpreted.
    value_render_option = 'RAW'  # TODO: Update placeholder value.

    request = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_) #, valueRenderOption=value_render_option, dateTimeRenderOption=date_time_render_option)
    complete = False
    while not complete:
        try:
            response = request.execute()
            complete = True
        except:
            sleep(cooldown())
    #print(response)
    return response['values']



def updatecells(cellloc, value):

    # Call the Sheets API
    # The ID of the spreadsheet to update.
    spreadsheet_id = MEM_SHEET_ID  # TODO: Update placeholder value.

    # The A1 notation of the values to update.
    range_ = 'Sheet1!'+cellloc+':'+cellloc  # TODO: Update placeholder value.

    # How the input data should be interpreted.
    value_input_option = 'RAW'  # TODO: Update placeholder value.

    value_range_body = {
            'values': [[str(value)]]
    }

    request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, body=value_range_body)
    complete = False
    while not complete:
        try:
            response = request.execute()
            complete = True
        except:
            sleep(cooldown())

    
def savestate(v1, v2):
   

    # Call the Sheets API
    # The ID of the spreadsheet to update.
    spreadsheet_id = MEM_SHEET_ID  # TODO: Update placeholder value.

    # The A1 notation of the values to update.
    range_ = 'Sheet1!A1:A2' #+cellloc  # TODO: Update placeholder value.

    # How the input data should be interpreted.
    value_input_option = 'RAW'  # TODO: Update placeholder value.

    value_range_body = {
            'values': [[str(v1)], [str(v2)]]
    }

    request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_, valueInputOption=value_input_option, body=value_range_body)
    complete = False
    while not complete:
        try:
            response = request.execute()
            complete = True
        except:
            sleep(cooldown())    

def appendtosheet(inlist):
    spreadsheet_id = MEM_SHEET_ID
    range_name = 'Sheet1!A1:A'
    mylist = [str(datetime.datetime.now()).split('.')[0]]
    mylist.extend(inlist)
    

    # Call the Sheets API
    values = [mylist]
    body = {
            'values': values
    }
    request = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption="RAW", body=body)
    #print(result['tableRange'])

    complete = False
    while not complete:
        try:
            result = request.execute()
            complete = True
        except:
            sleep(cooldown())
    print('{0} cells appended.'.format(result \
                                                                               .get('updates') \
                                                                               .get('updatedCells')))    
    return result['updates']['updatedRange'].split(':')[-1][1:]
    

@app.route('/worker/getID')
def give_id():
    complete = False
    while not complete:
        desnum = random.randint(1, 1000000)
        if desnum not in getworkers():
            addworker(desnum)
            complete = True
    #workeralive(desnum) (now included in addworker)
    return str(desnum)

@app.route('/worker/getBatch') #Parameters: id
def give_batch():
    id = request.args.get('id', '')
    #threading.Thread(target=workeralive, args=id).start()
    if id not in getworkers():
        return 'Fail'
    randomkey = random.randint(1, 10000)
    batchid = appendtosheet(['a', '', str(randomkey), str(id)])
    myj = {'batchID': batchid, 'randomKey': str(randomkey)}
    myresp = Response(json.dumps(myj), mimetype='application/json')
    return myresp

#@app.route('/worker/getExcludedBatch') #Parameters: id
#def give_batch():
#    id = request.args.get('id', '')
#    randomkey = random.randint(1, 10000)
#    batchid = appendtosheet(['a', '', str(randomkey), str(id)])
#    myj = {'batchID': batchid, 'randomKey': str(randomkey)}
#    myresp = Response(json.dumps(myj), mimetype='application/json')
#    return myresp
    
@app.route('/worker/submitExclusion')
def submit_exclusion(): #Parameters: id, batchID, randomKey, exclusion
    id = request.args.get('id', '')
    #threading.Thread(target=workeralive, args=id).start()
    if id not in getworkers():
        return 'Fail'
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    exclusion = request.args.get('exclusion', '')
    if not exclusion:
        return 'Fail'
    if getcell('D'+str(batchid)) == randomkey and getcell('E'+str(batchid)) == id: #valid submission
        cellc = getcell('C'+str(batchid))
        splitter = ','
        if not cellc:
            splitter = ''
        updatecells('C'+str(batchid), (cellc+splitter+str(exclusion)))
        return 'Success'
    else:
        return 'Fail'
    
@app.route('/worker/submitDeleted')
def submit_deleted(): #Parameters: id, batchID, randomKey, deleted
    id = request.args.get('id', '')
    #threading.Thread(target=workeralive, args=id).start()
    if id not in getworkers():
        return 'Fail'
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    deleted = request.args.get('deleted', '')
    if not deleted:
        return 'Fail'
    if getcell('D'+str(batchid)) == randomkey and getcell('E'+str(batchid)) == id: #valid submission
        cellf = getcell('F'+str(batchid))
        splitter = ','
        if not cellf:
            splitter = ''
        updatecells('F'+str(batchid), (cellf+splitter+str(deleted)))
        return 'Success'
    else:
        return 'Fail'
    
@app.route('/worker/submitPrivate')
def submit_private(): #Parameters: id, batchID, randomKey, private
    id = request.args.get('id', '')
    #threading.Thread(target=workeralive, args=id).start()
    if id not in getworkers():
        return 'Fail'
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    private = request.args.get('private', '')
    if not private:
        return 'Fail'
    if getcell('D'+str(batchid)) == randomkey and getcell('E'+str(batchid)) == id: #valid submission
        cellg = getcell('G'+str(batchid))
        splitter = ','
        if not cellg:
            splitter = ''
        updatecells('G'+str(batchid), (cellg+splitter+str(private)))
        return 'Success'
    else:
        return 'Fail'

@app.route('/worker/updateStatus')
def update_status(): #Parameters: id, batchID, randomKey, status ('a'=assigned,) 'c'=completed, 'f'=failed
    id = request.args.get('id', '')
    #threading.Thread(target=workeralive, args=id).start()
    if id not in getworkers():
        return 'Fail'
    batchid = request.args.get('batchID', '')
    randomkey = request.args.get('randomKey', '')
    status = request.args.get('status', '')
    if getcell('D'+str(batchid)) == randomkey and getcell('E'+str(batchid)) == id and (status in ['c', 'f']): #valid submission
        updatecells('B'+str(batchid), status)
        return 'Success'
    else:
        return 'Fail'

@app.route('/robots.txt')
def download_robots_txt():
    return Response('User-agent: *\nDisallow: /', mimetype='text/plain')

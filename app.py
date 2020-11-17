"""
Created on Mon Nov 16 01:15:31 2020
@author: Venkata N Divi
"""

from celery import Celery
from flask import Flask,request
import os,sys,requests,task,json
from config import MONGODB_CON_STR

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = MONGODB_CON_STR               #Adding Celery Broker URL to Flask App Config

celery = Celery(app.name,broker=app.config['CELERY_BROKER_URL'])#Creating a Celery Async Task Object
celery.conf.update(app.config)

headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
taskObj = task.MainTask()                                       #Creating an object for Main Task Class

@celery.task
def index_data_to_es(data):
    # This is Celery task which will take each request in Async and index data to Elastic Search
    # Step 1: Mark the task current status to in-progress
    # Step 2: Start the Elastic Search Data Sinking process
    try:
        with app.app_context():
            taskID,indexFile = data['taskID'],data['indexFile']
            requests.post("http://localhost:7004/updateRecord", data=json.dumps({'taskID': taskID, 'status': 'In-Progress'}), headers=headers)
            taskObj.sink_data_to_es(indexFile,taskID)
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
        
@app.route('/index',methods=['POST'])
def index():
    # This is the Flask Service which will accept a POST request from user and index data to Elastic Search
    # Step 1: Read data from JSON request and insert the task in DB with Queued Status
    # Step 2: Create an Celery Async Task
    # Step 3: Return the 200 Success Code to the user stating the index is in progress
    # Step 4: If any error or data issue in between will return a 500 status with description to the user 
    try:
        if request.json:
            data = {}
            if 'taskID' in request.json and 'indexFile' in request.json and '.csv' in request.json["indexFile"]:
                taskID,indexFile = request.json['taskID'],request.json["indexFile"] 
                if os.path.isfile(indexFile):
                    data['taskID'],data['indexFile'] = taskID,indexFile
                    requests.post("http://localhost:7004/updateRecord", data=json.dumps({'taskID': taskID, 'status': 'Queued'}), headers=headers)
                    index_data_to_es.apply_async(args=[data])
                    return { 'statusCode': 200, 'body': 'Data is getting added to ES!!!' }
            else:
                return { 'statusCode': 500, 'body': 'Check and resend the taskID and indexFile details' }
        else:
            return { 'statusCode': 500, 'body': 'Send the request in Proper JSON Format.' }
    except Exception as e:
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
        return { 'statusCode': 500, 'body': 'Unable to process Request' }


if __name__=='__main__':
    app.run(debug=True,host='localhost',port='7005')
"""
Created on Mon Nov 16 02:25:31 2020
@author: Venkata N Divi
"""

import csv,sys,requests,json,extract_keywords
from config import ELASTICSEARCH_URL,ELASTICSEARCH_INDEX
from elasticsearch import helpers, Elasticsearch#, RequestsHttpConnection

class MainTask():
    # This class contains methods to process input file and pushing data to Elastic Search
    
    def __init__(self):
        # Initialize all the required Elastic Search indexes, Objects for classes and header declarations
        
        self.indexName = ELASTICSEARCH_INDEX
        self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        #self.es = Elasticsearch([ELASTICSEARCH_URL], connection_class= RequestsHttpConnection, http_auth=('logstash', 'logstash'), use_ssl=True, verify_certs=False)
        self.es = Elasticsearch([ELASTICSEARCH_URL])
        self.keywordExtraction = extract_keywords.GetKeywords()
        
    def process_file(self,indexFile,taskID):
        # This method will process the input file and generate keywords for each input answer and return an 
        # Object with list of objects containing TaskID, Question, Answer and Keywords
        #
        # Step 1: Read the file line by line and send to keyword extraction class to get the keywords
        # Step 2: Create an object of lists which contains the original data with keywords appended
        # Step 3: Return the keywords object
        
        data = []
        try:
            with open(indexFile, newline='', encoding='utf-8') as fileData:
                reader = csv.reader(fileData, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                next(reader, None)
                for row in reader:
                    if len(row) == 2:
                        result = {}
                        question,answer = ' '.join(row[0].replace('\n','').split()),' '.join(row[1].replace('\n','').split())
                        keyWords = self.keywordExtraction.get_key_words(answer)
                        result['question'],result['answer'],result['keywords'] = question, answer, keyWords
                        result['taskID'] = taskID
                        data.append(result)
                
            return data
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
            return data
        
    def sink_data_to_es(self,indexFile,taskID):
        # This is the main method in this class which will process input file and generate keywords for answers
        # and create an object with taskID, Question, Answer and Keywords and push that object to Elastic Search
        # in Bulk and update the task status as Completed if success or Failed if failure
        #
        # Step 1: Take input arguments and pass to process file method
        # Step 2: Check if Elastic Search is up and running
        # Step 3: Push the data in bulk to Elastic Search
        # Step 4: Update the task status as Completed if success or Failed if fail.
        
        try:
            data = self.process_file(indexFile,taskID)
            if self.es.ping():
                try:
                    helpers.bulk(self.es, data, index=self.indexName, doc_type='doc')
                except Exception as e:
                    requests.post("http://localhost:7004/updateRecord", data=json.dumps({'taskID': taskID, 'status': 'Failed'}), headers=self.headers)
                    pass
                else:
                    requests.post("http://localhost:7004/updateRecord", data=json.dumps({'taskID': taskID, 'status': 'Successful'}), headers=self.headers)
            else:
                requests.post("http://localhost:7004/updateRecord", data=json.dumps({'taskID': taskID, 'status': 'Failed'}), headers=self.headers)
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
            requests.post("http://localhost:7004/updateRecord", data=json.dumps({'taskID': taskID, 'status': 'Failed'}), headers=self.headers)
            

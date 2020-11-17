# asyncService

This is a Flask based Celery Async Task Service which is created to index data to Elastic Search by processing the given CSV file by generating keywords for the data present in the CSV File.

***File Structure***

***app.py:*** This is the main file to run the Flask app and the Celery task which will read input request data by creating an async celery task and update the task ID in the database and return the response to the user

***task.py:*** This is the file which contains methods to process input file and pushing the data to Elastic Search

***extract_keywords.py:*** This is the file which contains the methods to generate important keyphrases/keywords from the imput text. I have used nltk, spacy for generating the keywords.

***config.py:*** This file contains all the celery broker details and elastic search server details.

This Service will run on localhost on port 7005

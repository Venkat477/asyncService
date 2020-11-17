"""
Created on Mon Nov 16 04:35:31 2020
@author: Venkata N Divi
"""

from spacy import load
import re,sys,itertools
from nltk import sent_tokenize
from nltk.corpus import stopwords
from unicodedata import normalize
from nltk.chunk import tree2conlltags
from nltk.chunk.regexp import RegexpParser
from nltk.stem.wordnet import WordNetLemmatizer

class GetKeywords():
    # This class contains methods to generate important keywords from an input string
    
    def __init__(self):
        # Initialize all the required stop words, lemmatizer, nlp objects, regex parser objects and grammar
        # declaration for extracting keywords from the text.
        
        self.grammar = r'NP: {<DT>|<JJ>|<AFX>? <HYPH>* <NN.*>+}'
        self.stopword_list = stopwords.words('english')
        self.lem = WordNetLemmatizer()
        self.nlp = load('en', disable=['parser', 'ner'])
        self.chunker = RegexpParser(self.grammar)

    def get_chunks(self,sentences):
        # This method will generate the required important keywords from the list of sentences
        #
        # Step 1: Lemmatize each sentence and extract POS tagging for each word in the sentence
        # Step 2: From the POS tagging, will extract all the true nouns and adjective combination of nouns
        # Word phrases.
        # Step 3: Clean and remove all the other POS tag words and frame only noun related keywords and return
        
        all_chunks = []
        try:
            for sentence in sentences: 
                sentence = ' '.join([self.lem.lemmatize(word) for word in sentence.split()])
                if len(sentence) > 1 :
                    posTag,pos = [],[]
                    document = self.nlp(str(sentence))
                    [pos.append((str(phrase), phrase.tag_)) for phrase in list(document)]  
                    posTag.append(pos)
                    
                    chunks = [self.chunker.parse(tagged_sent) for tagged_sent in posTag]
                    wtc_sents = [tree2conlltags(chunk) for chunk in chunks]    
                    flattened_chunks = list(itertools.chain.from_iterable(wtc_sent for wtc_sent in wtc_sents))
                    valid_chunks_tagged = [(status, [wtc for wtc in chunk]) for status, chunk in itertools.groupby(flattened_chunks,lambda word__pos__chunk: word__pos__chunk[2] != 'O')]
                    valid_chunks = [' '.join(word.lower() for word, tag, chunk in wtc_group if word.lower() not in self.stopword_list) for status, wtc_group in valid_chunks_tagged if status]
                    all_chunks.append(valid_chunks)
                    
            return all_chunks
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
            return all_chunks
        
    def get_keyphrases(self,sentences):
        # This method will pass the list of sentences as an arguments to extract major key phrases
        
        try:
            valid_chunks = self.get_chunks(sentences)
            return valid_chunks[0]
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
            return []
            
    def parse_document(self,text):
        # This method will encode the input text and divide the sentence to individual tokens to extract the 
        # important keywords.
        
        try:
            document = re.sub('\n', ' ', text)
            if not isinstance(document, str): document = normalize('NFKD', document).encode('ascii', 'ignore')
            document = document.strip()
            sentences = sent_tokenize(document)
            sentences = [sentence.strip() for sentence in sentences]
            return sentences
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
            return None
        
    def get_key_words(self,text):
        # This method will clean the input text by removing all the unnecessary characters and call the
        # necessary methods to extract the keywords from the text
        #
        # Step 1: Clean the text by removing all the unnecessary characters
        # Step 2: Extract the keywords by calling the necessary methods
        # Step 3: If the extracted keywords are less then 10, then return all the keywords to the user
        # else return the keywords by omitting uni-grams.
        
        try:
            text = str(text)        
            text = re.sub("&lt;/?.*?&gt;"," ",text)
            text = re.sub("(\\d|\\W)+"," ",text)
            text = re.sub(r'(?:http[s]?:\/\/)?(?:www\.)?([^/\n\r\s]+\.[^/\n\r\s]+)(?:/)?(\w+)?','',text)
            text = re.sub('[^A-Za-z.\' ]+', ' ', text)
            text = ' '.join(text.replace('\n',' ').strip().split()).lower()
            
            sentences = self.parse_document(text) 
            if sentences and len(sentences) > 0 :
                titleChunk = self.get_keyphrases(sentences)
                keyWords = [phrase for phrase in titleChunk if len(phrase)>0]
                processedWords = [' '.join(list(set(word.split()))) for word in keyWords]
                if len(processedWords)>10:
                    processedWords = [word for word in processedWords if ' ' in word]
                return (list(set(processedWords)))
            else: return []
        except Exception as e:
            print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno),Exception, e)
            return []
    

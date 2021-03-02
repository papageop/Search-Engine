from pymongo import MongoClient
from dotenv import load_dotenv
import os

class MongoDB:
    '''
    This class is used for the interoperability of the subsystems of the search engine 
    and the database that contains the documents and the terms that have been crawled.
    '''
    def __init__(self):
        load_dotenv()  # load enviromental variables fro
        username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
        self.database = os.getenv("MONGO_INITDB_DATABASE")
        ip = os.getenv("MONGO_IP")
        self.client = MongoClient(ip, username=username, password=password, authSource="admin")[self.database]
        self.crawler_db = self.client.crawler_records
        self.documents_db = self.client.documents
        self.indexer_db = self.client.index
     
    def reset_crawler(self):
        '''
        This method drops the database tables of the crawler for all the crawled documents
        and resets the database every time the user runs the crawler demanding to delete previous pages.
        '''
        self.crawler_db.drop()
        self.crawler_db = self.client.crawler_records

    def find_all_crawler_records(self):
        '''
        This method retrieves and returns all crawled documents.
        '''
        return self.crawler_db.find({}, no_cursor_timeout=True)


    def reset_indexer(self):
        '''
        This method drops the database tables of the indexer and the documents
        and resets the inverted index.
        '''
        self.indexer_db.drop()
        self.documents_db.drop()
        self.indexer_db = self.client.index
        self.documents_db = self.client.documents

    def build_documents_db(self):
        '''
        This method is used as the first step for building the inverted index.
        All the crawled documents are inserted into the documents database.
        '''
        self.documents_db.insert_many(self.find_all_crawler_records())

    def is_initialized(self):
        '''
        This method looks if the documents and index collection of the inverted index are created
        for the query handler to run and return results. If they are not initialized and
        the user runs the flask server, an error message page is returned to the user. 
        '''
        collections = self.client.list_collection_names()
        if "documents" in collections and "index" in collections:
            return True
        else:
            return False

    def get_documents_count(self):
        '''
        This method returns the number of documents in the database.
        '''
        return self.documents_db.count()

    def find_document_by_id(self, d_id):
        '''
        This method searches and returns the document entry with the document ID 
        given as parameter.
        '''
        return self.documents_db.find_one({"_id": d_id})

    def find_document_ids(self):
        '''
        This method returns the IDs of all the documents in the documents database. 
        '''
        mongo_results = self.documents_db.find({}, {"_id": 1})
        return [item["_id"] for item in mongo_results]

    def add_doc_length(self, doc_id, doc_length):
        '''
        This method adds a new field for every document that contains the 
        length of the document needed for the Query Handler to compute the
        similarity between a document and the query.
        '''
        self.documents_db.update({"_id": doc_id}, {"$set": {"length": doc_length}})

    def add_to_indexer(self, data):
        '''
        This method adds a new term in the inverted index with the term frequency (t_freq) and an array
        of the documents that contain this term 
        '''
        self.indexer_db.insert_one(data)

    def update_indexer(self, term, new_data):
        '''
        Τhis method requires two parameters: the term
        '''
        entry = self.indexer_db.find_one({"term": term})
        t_freq = entry["t_freq"] + 1
        documents = entry["documents"]
        documents.append(new_data)
        self.indexer_db.update({"term": term},
                               {"$set": {"t_freq": t_freq, "documents": documents}})

    def find_term_in_index(self, term):
        '''
        This method looks for a keword in the index table and returns the entry data
        '''
        return self.indexer_db.find_one({"term": term})


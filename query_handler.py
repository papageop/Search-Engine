from mongodb import MongoDB
import threading
import time
import math


class QueryHandler:
    def __init__(self, num_threads_array=5):
        self.thread_array = []
        self.num_threads = num_threads_array
        self.db = MongoDB()
        self.num_docs = 0
        self.scores = {}
        self.query = {}
        self.locker = threading.Lock()
        self.relevantLocker = threading.Lock()
        self.nonRelevantLocker = threading.Lock()

    def main(self, query, k):
        '''
        This method calculates the query results calculating the score of documents using cosine similarity formula.
        '''
        print("We are processing your query...")
        top_k = k
        self.scores = {} #document scores
        
        
        self.scores.clear()
        self.num_docs = self.db.get_documents_count() #number of documents in database

        query_keywords = [keyword.lower() for keyword in query] #query keywords to lowercase
        self.query_kewords = query_keywords
        #loop for every term in query
        for term in query_keywords:
            word = self.db.find_term_in_index(term)
            if word is not None:
                t_freq = word["t_freq"]

                for document in word["documents"]:
                    
                    while sum([1 for t in self.thread_array if t.is_alive()]) > 1:
                        time.sleep(0.5)

                    new_task = threading.Thread(
                        target=self.score, args=(document, t_freq, ))
                    new_task.start()
                    self.thread_array.append(new_task)
        #Wait all threads to finish
        while sum([1 for t in self.thread_array if t.is_alive()]) > 0:
            time.sleep(0.5)

        # Normalize the document scores using the document length
        for doc_id in self.scores.keys():
            retrieved_doc = self.db.find_document_by_id(doc_id)
            if retrieved_doc is not None:
                doc_length = retrieved_doc["length"]
                self.scores[doc_id] = self.scores[doc_id] / doc_length

        # Sort scores in descending order
        scores = {k: v for k, v in sorted(
            self.scores.items(), key=lambda x: x[1], reverse=True)}

        # Get documents with the k best scores
        query_results = []
        k = 0
        for doc_id in scores.keys():
            k += 1
            document = self.db.find_document_by_id(doc_id)
            query_results.append(
                {"_id": document["_id"], "title": document["title"], "url": document["url"]})
            if k == top_k:
                break
        print("Query Handler finished!")

        return query_results

    def score(self, document, term_freq):
        '''
        This method calculates the score of a document for a given term frequency using TF-IDF 
        formula and cosine similarity.
        '''
        id = document["_id"]
        if id not in self.scores.keys():
           
            with self.locker:
                self.scores[id] = 0
        tf = 1+math.log(term_freq)
        idf = math.log(1+(self.num_docs/term_freq))
        with self.locker:
            self.scores[id] = self.scores[id] + tf * idf

    def computeRelevantTF(self, doc):
        '''
        This method calculates the term summation of term frequencies in a document for 
        all documents declared as relevant
        '''
        for new_term in doc["bag"]:
            term = self.db.find_term_in_index(new_term)
            with self.relevantLocker:
                if new_term in self.relevantDocsTF:
                    self.relevantDocsTF[new_term] = self.relevantDocsTF[new_term] + \
                        term["t_freq"]
                else:
                    self.relevantDocsTF[new_term] = term["t_freq"]

    def computeNonRelevantTf(self, doc):
        '''
        This method calculates the term summation of term frequencies in a document for 
        all documents declared as not relevant
        '''
        for new_term in doc["bag"]:
            term = self.db.find_term_in_index(new_term)
            with self.nonRelevantLocker:
                if new_term in self.nonrelevantDocsTF:
                    self.nonrelevantDocsTF[new_term] = self.nonrelevantDocsTF[new_term] + \
                        term["t_freq"]
                else:
                    self.nonrelevantDocsTF[new_term] = term["t_freq"]

    def rocchio_relevance_feedback(self, relevantDocs, nonrelevantDocs):
        '''
        This method returns a new query vector calculated using Rocchio formula 

        '''

        weights = {}
        for term in self.db.indexer_db.find({}):

            # initialize weight vector for each key in inverted file
            weights[term["term"]] = 0.0
        alpha = 0.5
        beta = 0.7
        gamma = 0.1
        for term in self.query_kewords:
            self.query[term] = 1.0

        self.relevantDocsTF = {}
        self.nonrelevantDocsTF = {}

        # ------------------------------------- #
        # Compute relevantDocsTF and nonrelevantDocsTF vectors
        for docId in relevantDocs:
            doc = self.db.find_document_by_id(docId)

            # Wait until thread pool has an available thread
            while sum([1 for t in self.thread_array if t.is_alive()]) > self.num_threads:
                time.sleep(0.5)

            new_task = threading.Thread(
                target=self.computeRelevantTF, args=(doc, ))
            new_task.start()
            self.thread_array.append(new_task)
            '''
            for new_term in doc["bag"]:
                term=self.db.find_term_in_index(new_term)
                

                if new_term in relevantDocsTF:
                    relevantDocsTF[new_term] = relevantDocsTF[new_term] + term["t_freq"]
                else:
                    relevantDocsTF[new_term] = term["t_freq"]
'''

        for docId in nonrelevantDocs:
            doc = self.db.find_document_by_id(docId)
            # Wait until thread pool has an available thread
            while sum([1 for t in self.thread_array if t.is_alive()]) > self.num_threads:
                time.sleep(0.5)

            new_task = threading.Thread(
                target=self.computeNonRelevantTf, args=(doc, ))
            new_task.start()
            self.thread_array.append(new_task)
            '''
            for new_term in doc["bag"]:
                term=self.db.find_term_in_index(new_term)
                             

                if new_term in nonrelevantDocsTF:
                    nonrelevantDocsTF[new_term] = nonrelevantDocsTF[new_term] + term["t_freq"]
                else:
                    nonrelevantDocsTF[new_term] = term["t_freq"]

'''
        # ------------------------------------- #
        # Compute Rocchio vector
        for term in self.db.indexer_db.find({}):
            new_term = self.db.find_term_in_index(term["term"])
            idf = math.log(float(self.db.get_documents_count()) /
                           float(len(new_term["documents"])), 10)
            new_term = new_term["term"]

            # Terms 2 and 3 of Rocchio algorithm

            if new_term in self.relevantDocsTF:
                # Term 2: Relevant documents weights normalized and given BETA weight
                weights[new_term] = weights[new_term] + beta * idf * \
                    (self.relevantDocsTF[new_term] / len(relevantDocs))
            if new_term in self.nonrelevantDocsTF:
                # Term 3: NonRelevant documents weights normalized and given BETA weight
                weights[new_term] = weights[new_term] - gamma * idf * \
                    (self.nonrelevantDocsTF[new_term]/len(nonrelevantDocs))
            # Term 1 of Rocchio, query terms

            if term in self.query_kewords:
                self.query[new_term] = alpha * self.query[new_term] + \
                    weights[new_term]  # build new query vector of weights
            elif weights[new_term] > 0:
                self.query[new_term] = weights[new_term]
        self.query = {k: v for k, v in sorted(
            self.query.items(), key=lambda x: x[1], reverse=True)}

        return self.query

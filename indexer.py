import math
import threading
import time

from mongodb import MongoDB


class Indexer:

    def __init__(self, num_threads_array=2):
        self.thread_array = []
        self.index = {}
        self.num_threads = num_threads_array
        self.db = MongoDB()

    def create_index(self):
        '''
        This method builds the inverted index 
        '''
        print("Creating inverted index...")
        t1 = time.perf_counter()
        self.db.reset_indexer()
        self.db.build_documents_db()
        # Get the documents total count
        self.docs_count = self.db.get_documents_count()
        # Get all document IDs from the database
        self.doc_ids = self.db.find_document_ids()

        for doc_id in self.doc_ids:
            document = self.db.find_document_by_id(doc_id)
            bag = document["bag"]
            for term in bag:
                while sum([1 for t in self.thread_array if t.is_alive()]) > self.num_threads:
                    time.sleep(0.5)
                new_task = threading.Thread(
                    target=self.process_term, args=(document, term))
                new_task.start()
                self.thread_array.append(new_task)
        #Wait all threads to finish
        while sum([1 for t in self.thread_array if t.is_alive()]) > 0:
            time.sleep(0.5)

        self.thread_array = []

        for _id in self.doc_ids:

            document = self.db.find_document_by_id(_id)
            while sum([1 for t in self.thread_array if t.is_alive()]) > self.num_threads:
                time.sleep(0.5)

            new_task = threading.Thread(
                target=self.calculate_doc_length, args=(document, ))
            new_task.start()
            self.thread_array.append(new_task)
        #Wait all threads to finish
        while sum([1 for t in self.thread_array if t.is_alive()]) > 0:
            time.sleep(0.5)
        t2 = time.perf_counter()
        print("Inverted Index is successfully created. Total time {total}...".format(
            total=t2-t1))

    def process_term(self, document, term):
        '''
        This method looks if the term exists in the database and updates or adds it to the database
        '''
        id = document["_id"]
        url = document["url"]
        title = document["title"]
        bag = document["bag"]

        # Check if the term already exists in the inverted index and update the database
        if self.db.find_term_in_index(term):
           
            self.db.update_indexer(term, {"_id": id,
                                          "title": title,
                                          "url": url,
                                          "t_d_freq": bag[term]
                                          })
        else:
            # If the term does not exist in the index, add it to the database
            self.db.add_to_indexer({"term": term.lower(),
                                    "t_freq": 1,
                                    "documents": [{"_id": id,
                                                   "title": title,
                                                   "url": url,
                                                   "t_d_freq": bag[term]}]
                                    })

    def calculate_doc_length(self, document):
        '''
        This method is used to calculate the document length
        '''
        doc_id = document["_id"]
        bag = document["bag"]
        # Initialize the score accumulator for the current document to 0
        squared_weights_sum = 0

        # Find maximum term-document frequency value for this document
        max_t_d_freq = 1
        for word in bag:
            term = self.db.find_term_in_index(word)
            t_d_freq = 0
            for document in term["documents"]:
                if document["_id"] == doc_id:
                    t_d_freq = document["t_d_freq"]

            if t_d_freq > max_t_d_freq:
                max_t_d_freq = t_d_freq
        # for every word in the bag of the document calculate normalized tf-idf weight
        for word in bag:
            term = self.db.find_term_in_index(word)
            t_d_freq = 0
            for document in term["documents"]:
                if document["_id"] == doc_id:
                    t_d_freq = document["t_d_freq"]
            tf = term["t_freq"]

            norm_t_d_freq = t_d_freq / max_t_d_freq
            
            nidf = math.log(self.docs_count / tf) / \
                math.log(self.docs_count)
            

            squared_weights_sum += math.pow(nidf * nidf, 2)

        # Calculate document's length and add it to dictionary.
        length = math.sqrt(squared_weights_sum)

        self.db.add_doc_length(doc_id, length)

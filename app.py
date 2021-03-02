from flask import Flask, render_template, url_for, request, redirect
import sys
from mongodb import MongoDB
from query_handler import QueryHandler

app = Flask(__name__)
mongo = MongoDB()


@app.route('/', methods=['POST', 'GET'])
def index():
    
    index_initialized = mongo.is_initialized()
    if index_initialized:

        if request.method == 'POST':
            content = request.form['content'] #query keywords
            ids = request.form['ids'] #IDs of relevant documents to compute new query using Rocchio formula 
            top_k = int(request.form['top-k']) #num of top k documents to return
            ids = ids.split()
            ids = [int(i) for i in ids]
            
            # Split query string into individual words using whitespace as the delimiter
            query_keywords = content.split()

            # Execute the query using the Search Engine's Query Handler

            query_results = query_handler.main(query_keywords, top_k)
            rel = [query_results[i]["_id"]
                   for i in range(len(query_results)) if i in ids]
            nonrel = [query_results[i]["_id"]
                      for i in range(len(query_results)) if i not in ids]
            #if the user has selected relevant document IDs compute new query with Rocchio formula
            if ids != []:
                q = query_handler.rocchio_relevance_feedback(rel, nonrel)
                i=0
                new_query=[]
                for k in q.keys():
                    new_query.append(k)
                    i=i+1 
                
            #if the user has selected relevant document IDs get new query results
            if ids != []:
                query_results = query_handler.main(new_query, top_k)
            for i in range(len(query_results)):
                query_results[i]["num"] = i

            return render_template('index.html', webpages=query_results, len=top_k, query=content)

        else:
            return render_template("index.html", webpages=[])
    else:
        return render_template("error.html")


if __name__ == "__main__":
    '''
    Main method to run flask server and initialize a QueryHandler object
    '''
    query_handler = QueryHandler(int(sys.argv[1]))
    print("Starting Flask Server...")
    app.run(debug=True)

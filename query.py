import multiprocessing as mp
import time, re
from textblob import TextBlob, Word
from nltk.corpus import stopwords

class QueryClass:
    def print_d(self, *msg):
        if self.debug:
            print("Query Engine:",*msg)


    def print_v(self, *msg):
        if self.verbose:
            print("Query Engine:",*msg)


    def __init__(self, reached_limit, documents_indexed, index_lock, allowed_address, port, debug=False, verbose=True):
        self.index_lock, self.allowed_address, self.port = index_lock, allowed_address, port
        self.debug, self.verbose = debug, verbose
        self.reached_limit, self.documents_indexed = reached_limit, documents_indexed
        #Queries type : political, commercial, technical, casual, fact question      
        self.queries = [
                "Donald Trump news",
                "party dresses",
                "technology progress",
                "delicious food"]
        return
        self.queryLoop()


    def queryLoop(self):
        stop_words = set(stopwords.words('english'))
        while not self.reached_limit.value:
            time.sleep(10)
        self.print_v("The crawler has finished its initial run")
        while not self.documents_indexed.value:
            time.sleep(10)
        self.print_v("The indexer has finished its indexing job")

        #Load the indexes
        self.index_lock.value = True
        self.IDF, self.BM25, self.pageRank = {}, {}, {}
        with open('words_index', 'r', encoding='utf-8') as words_index: #inverse index (BM25 weights)
            self.print_d("Loading words index file")
            for line in words_index:
                l = line.split('?')
                self.IDF[l[0]] = float(l[1])
                self.BM25[l[0]] = {}
                for url_weight in [pair.split('@') for pair in l[2:]]:
                    self.BM25[l[0]][url_weight[0]] = float(url_weight[1])
        with open('documents_stats', 'r', encoding='utf-8') as documents_stats: #individual documents stats
            self.print_d("Loading documents stats file")
            for line in documents_stats:
                l = line.split('?')
                self.pageRank[l[0]] = float(l[1])
        self.print_d(self.pageRank)
        self.index_lock.value = False
        self.print_v("Indexes ready for querying")

        #Get the BM25 scores
        alphanum_pattern = re.compile('[\W_]+', re.UNICODE) #yup, underscores are alphanum for re
        results = {}
        queries_keywords = {}
        for q in self.queries:
            queries_keywords[q] = set() 
            results[q] = {}
            start_query = time.clock() #ms
            self.print_v("Fetching results for query : {}".format(q))
            blob = TextBlob(alphanum_pattern.sub(' ', q.lower()))
            for term in blob.words:
                w = term.singularize()
                if w in self.IDF and w not in stop_words:
                    queries_keywords[q].add(str(w))
                    for doc in self.BM25[w]:
                        if doc in results:
                            results[q][doc] += self.IDF[w] * self.BM25[w][doc]
                        else:
                            results[q][doc] = self.IDF[w] * self.BM25[w][doc]
            self.print_v("Execution time : {} ms".format(int(time.clock() - start_query)*1000))
        
        #Rank by BM25 score
        top = 20 #arbitrary
        bm25_results = {}
        for q in results:
            bm25_results[q] = {}
            count = 0
            top_results = {}
            for doc in sorted(results[q], key=lambda d: results[q][d], reverse=True):
                top_results[doc] = results[q][doc] 
                count += 1
                if count <= 10:
                    bm25_results[q][doc] = results[q][doc]
                if count >= top:
                    break
            results[q] = top_results
            print(results[q])
                
        #Rerank by PageRank
        top = 10 #for evaluation purpose
        for q in results:
            count = 0
            top_results = {}
            for doc in sorted(results[q], key=lambda d: self.pageRank[d], reverse=True):
                if results[q][doc] > 0:               
                    top_results[doc] = results[q][doc] 
                count += 1
                if count >= top:
                    break
            results[q] = top_results
        self.print_d("Top 10 - PR + BM25: ",results)

        #Get queries and results to file
        results_keys = list()
        for q in results:
            results_keys.extend(list(results[q].keys()))
        url_details = dict.fromkeys(results_keys, dict())
        self.print_d(url_details)
        count = len(url_details) 
        with open('text_index', 'r') as text_index: #get the text offset in the crawled docs
            for line in text_index:
                if not count:
                    break
                for u in url_details:
                    if u in line:
                        url_details[u]['offset'] = int(line.split('?')[1])
                        count -= 1
                        break
        self.print_d(url_details)
        with open('crawled_documents', 'r') as crawled_documents:
            for url in sorted(url_details, key=lambda u: url_details[u]['offset']): #sort by offset
                crawled_documents.seek(url_details[url]['offset'])
                l = crawled_documents.readline()
                nf = 4 #url, time, text, links
                headhead, head, data = l[:nf], list(), list()
                for f in headhead:
                    head.append(l[nf:nf+int(f)])
                    nf += int(f)
                for f in head:
                    data.append(l[nf:nf+int(f)]) #url, domain, text, links
                    nf += int(f)
                url_details[url]['text'] = data[2]
        
        #Seek snippet and print results
        query_results = open('query_results','w+', encoding='utf-8')
        for q in self.queries:
            result_string = '\nQuery results :' + q
            for r in sorted(results[q], key=lambda d: results[q][d], reverse=True): #best score first
                result_string += '\n\n' + r 
                snippet = ''
                for k in queries_keywords[q]:
                    regex = re.compile(r"\b{}e?s?\b".format(k))
                    pos = regex.search(url_details[r]['text'].lower())
                    if not pos:
                        self.print_d("Keyword ",k," not in", r)
                        continue
                    pos = pos.start(0)
                    pos2 = (pos+400) if len(url_details[r]['text'][pos:]) > 400 else (len(url_details[r]['text'][pos:]) - 1)
                    if pos:
                        result_string += '\n\t' + url_details[r]['text'][pos:pos2]
                        break

            self.print_v(result_string, "\n")
            query_results.write(result_string)
        query_results.close()

        bm25_file = open('bm25_results','w+', encoding='utf-8')
        for q in bm25_results:
            result_string = '\nQuery results :' + q
            for r in sorted(bm25_results[q], key=lambda d: bm25_results[q][d], reverse=True): #best score first
                result_string += '\n\n' + r 
                snippet = ''


            self.print_v(result_string, "\n")
            bm25_file.write(result_string)
        bm25_file.close()

        
        

def Query(reached_limit, documents_indexed, index_lock, allowed_address, port, debug=False, verbose=True):
    print("Started Query Engine")
    query_instance = QueryClass(reached_limit, documents_indexed, index_lock, allowed_address, debug, verbose)

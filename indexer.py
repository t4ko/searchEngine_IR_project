import multiprocessing as mp
import time, re
from textblob import TextBlob
from nltk.corpus import stopwords
import math

class IndexerClass:
    def print_d(self, *msg):
        if self.debug:
            print("Indexer:",*msg)


    def print_v(self, *msg):
        if self.verbose:
            print("Indexer:",*msg)


    def __init__(self, documents_lock, documents_indexed, documents_folder, index_lock, debug=False, verbose=True):
        self.docs_lock, self.documents_indexed, self.index_lock = documents_lock, documents_indexed, index_lock
        self.docs_folder = documents_folder
        self.debug, self.verbose = debug, verbose

        self.docs = {}
        self.collection = { "words":{}, "avg_length":0, "IDF":{}, "size":0, "count":{} }
        self.indexerLoop()


    def indexerLoop(self):
        stop_words = set(stopwords.words('english'))
        alphanum_pattern = re.compile('[\W_]+', re.UNICODE) #yup, underscores are alphanum for re
        repeat = 0
        total_read_docs_timer = 0
        total_terms_weight_timer = 0
        total_pagerank_timer = 0
        total_write_index_timer = 0

        while repeat < 10 and not self.documents_indexed.value:
            if True:
                self.docs_lock.value = 1 #start reading crawled docs
                read_docs_timer = time.clock()
                with open('crawled_documents', 'r', encoding='utf-8') as input_file:
                    for l in input_file:
                        nf = 4 #url, time, text, links
                        headhead, head, data = l[:nf], list(), list()
                        for f in headhead:
                            head.append(l[nf:nf+int(f)])
                            nf += int(f)
                        for f in head:
                            data.append(l[nf:nf+int(f)])
                            nf += int(f)
                        
                        #Extract terms frequency
                        if data[0] not in self.docs:
                            self.docs[data[0]] = { "words":{}, "doc_length":0, "pagerank":0, "inlinks":set(), "outlinks":set() }
                        blob = TextBlob(alphanum_pattern.sub(' ', data[2]).lower())
                        text = list(map(lambda w: w.singularize(), #singularize words
                                filter(lambda w: len(w) > 2, blob.words))) #Outs less than 2 char
                        for w in set(text) - stop_words: #Outs stopwords
                            wordCount = text.count(w)
                            self.docs[data[0]]["words"][w] = wordCount
                            self.docs[data[0]]["doc_length"] += wordCount
                        for w in self.docs[data[0]]["words"]:
                            if w in self.collection['count']: #if it has no been added yet add it everywhere
                                self.collection["count"][w] += 1
                                self.collection["words"][w] += self.docs[data[0]]["words"][w]
                            else:
                                self.collection["count"][w] = 1
                                self.collection["words"][w] = self.docs[data[0]]["words"][w]
                        self.collection["avg_length"] += self.docs[data[0]]["doc_length"]
                        self.collection["size"] += 1

                        #Extract outlinks
                        url_set = set(data[3].split('?'))
                        self.docs[data[0]]["outlinks"] = url_set

                        #Extract links
                        for l in url_set:
                            if l in self.docs:
                                self.docs[l]['inlinks'].add(l)
                            else:
                                self.docs[l] = {"words":{},"doc_length":0,"pagerank":0,"inlinks":set([data[0]]),"outlinks":set()}
                total_read_docs_timer += time.clock() - read_docs_timer
                self.print_v("Loaded {} documents".format(self.collection["size"]))
                self.docs_lock.value = 0 #stop reading crawled docs

                #Remove empty entries
                keys = list(self.docs.keys())
                for d in keys:
                    if not self.docs[d]["doc_length"]: #remove empty entries created from inlinks
                        del self.docs[d]
                        continue

                #Calculate terms weight
                terms_weight_timer = time.clock()
                self.print_v("Calculating terms weight")
                self.collection["avg_length"] /= self.collection["size"]
                for w in self.collection["words"]:
                    w_count = self.collection["count"][w]
                    c_size = self.collection["size"]
                    self.collection["IDF"][w] = math.log(( c_size - w_count + 0.5)/(w_count + 0.5),2)
                k1, b = 1.2, 0.75
                kp1, kt1mb, ktboa = k1 + 1, k1*(1-b), k1*b/self.collection["avg_length"]
                self.index = dict.fromkeys([w for w in self.collection["words"]], {})
                for d in self.docs:
                    for w in self.docs[d]["words"]:
                        freq = self.docs[d]["words"][w]
                        self.index[w][d] = (freq * kp1)/(freq + kt1mb + ktboa*self.docs[d]["doc_length"])
                total_terms_weight_timer += time.clock() - terms_weight_timer

                #Calculate PageRank scores
                self.print_v("Calculating pagerank scores")
                pagerank_timer = time.clock()
                initialScore = 1/self.collection["size"]
                factor = 0.85
                damping = (1 - factor)/self.collection["size"]
                for d in self.docs: #Initialize pagerank values
                    self.docs[d]["pagerank"] = initialScore
                temp, change = {}, 1
                while change > 0.001:
                    change = 0
                    for d in self.docs:
                        temp[d] = damping + factor * sum([self.docs[l]["pagerank"]/len(self.docs[l]["outlinks"]) for l in self.docs[d]['inlinks']])
                    for d in temp:
                        self.docs[d]["pagerank"] = temp[d]
                        change += abs(self.docs[d]["pagerank"] - temp[d])
                    change /= self.collection["size"]
                self.print_v([self.docs[d]["pagerank"] for d in self.docs])
                total_pagerank_timer += time.clock() - pagerank_timer

                #Write the indexes to their file
                write_index_timer = time.clock()
                self.print_v("Writing down the indexes")
                words_index = open('words_index', 'w+', encoding="utf-8") #inverse index (BM25 weights)
                documents_stats = open('documents_stats', 'w+', encoding="utf-8") #individual documents stats
                for w in self.index:
                    bm25_weights = ["{}@{}".format(url, self.index[w][url]) for url in self.index[w]]
                    words_index.write('{}?{}?{}\n'.format(w, self.collection['IDF'][w], '?'.join(bm25_weights)))
                for d in self.docs:
                    documents_stats.write("{}?{}\n".format(d, self.docs[d]["pagerank"]))
                words_index.close()
                documents_stats.close()
                total_write_index_timer += time.clock() - write_index_timer
                
                #self.documents_indexed.value = 1
                self.print_v("Finished indexing new documents")
                repeat += 1
            else:
                time.sleep(1) #wait for new crawled docs to be saved to disk
            pass

        #Print timers results
        print("Timers results for 100 pages\n")
        print("read docs : {}".format(total_read_docs_timer))
        print("term weight : {}".format(total_terms_weight_timer))
        print("pagerank : {}".format(total_pagerank_timer))
        print("write index : {}".format(total_write_index_timer))

def Indexer(documents_lock, documents_indexed, documents_folder, index_lock, debug=False, verbose=True):
    print("Started Indexer")
    indexer_instance = IndexerClass(documents_lock, documents_indexed, documents_folder, index_lock, debug, verbose)

########## Notes :
# ¤ Indexer must handle pageRank & BM25, it means document representation should include webgraph for in and out link
# also include the term frequency
# ¤ Query engine should be contacted through a socket and return the results as json
# ¤ Interface should send a query to the query engine through a socket as a json object and receive the response as json
# it will then format the response according to the different field of the answer's json
##########

import multiprocessing as mp
from crawler import Crawler
from indexer import Indexer
from query import Query

if __name__ == '__main__':
    #Parameters for crawler (Stop criterion, refresh rate, ...)
    debug, verbose = True, True
    max_domain_pages = 50 #100
    max_domain = 1000 #100
    max_depth = 3 #depth 0 is the seed
    update_interval = 20 #minutes
    seed_page = "www.liensutiles.org/sites.htm"
    documents_folder = "./documents"
    #Parameters for indexer

    #Parameters for query engine, Not implemented yet
    allowed_address = "192.168.1.104"
    port = 2560

    #Interface ?? (html page) Not implemented yet

    #Start a process for each component (except interface, see webserver for this)
    reached_limit = mp.Value('i', False, lock=True)
    documents_lock = mp.Value('i', False, lock=True)
    documents_indexed = mp.Value('i', True, lock=True)
    index_lock = mp.Value('i', False, lock=True)

    crawler_process = mp.Process(target=Crawler, args=(reached_limit, documents_lock, documents_indexed, documents_folder,
            max_domain_pages, max_domain, max_depth, update_interval, seed_page, debug, verbose))
    indexer_process = mp.Process(target=Indexer, args=(documents_lock, documents_indexed, documents_folder, index_lock, debug, verbose))
    query_process = mp.Process(target=Query, args=(reached_limit, documents_indexed, index_lock, allowed_address, port, debug, verbose))

    crawler_process.start()
    indexer_process.start()
    query_process.start()

    crawler_process.join()
    indexer_process.join()
    query_process.join()



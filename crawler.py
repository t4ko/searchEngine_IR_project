import multiprocessing as mp
import fetcher
from bs4 import BeautifulSoup
from contextlib import closing
import time, re, urllib, sys, os



def format_url(scheme, host, path, url):
    if '/' in url[0]: #Path starts from hostname
        url = scheme + '://' + host + url
        print(url)
    elif '../' in url[:3]: #recursive path
        url_components = url.split('/')
        path_components = path.split('/')
        url = scheme + "://" + host
        i, j = len(path_components) - 1, 0 #First path component that is not the file
        for u in url_components:
            if u == '..':
                i -= 1
                j += 1
        url = url + '/' + '/'.join(path_components[0:i]) + '/' + '/'.join(url_components[j:])
    elif 'http' not in url[:5]:
        url = scheme + "://" + host + '/'.join(path.split('/')[:-1]) #Remove previous filename

    return url[:8] + re.sub('//+', '/', url[8:]) #remove extra slash


def get_domain_name(hostname):
    dn = hostname.split('.')
    for i in reversed(range(0, len(dn) - 1)):
        if len(dn[i]) > 2:              
            return '.'.join(dn[i:-1])
    return ""  

def get_file_extension(url): #We assume the url is formatted correctly and has no paramete
    splitted = urllib.parse.urlparse(url).path.split('/')
    if len(splitted) > 0 and len(splitted[-1]) > 0: #There is a slash and something after
        splitted = splitted[-1].split('.')
        if len(splitted) > 0:
            return splitted[-1]
    return ''

def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    print('EXCEPTION IN ({}, LINE {}): {}'.format(filename, lineno, exc_obj))

class CrawlerClass:
    def print_d(self, *args):
        if self.debug:
            print("Crawler:",*args)


    def print_v(self, *args):
        if self.verbose:
            print("Crawler:",*args)


    def __init__(self, reached_limit, documents_lock, documents_indexed, documents_folder, max_domain_pages, 
            max_domain, max_depth, update_interval, seed_page, debug=False, verbose=True):
        print("Started Crawler")
        self.documents_lock, self.documents_indexed, self.documents_folder = documents_lock, documents_indexed, documents_folder
        self.max_domain_pages, self.max_domain, self.max_depth = max_domain_pages, max_domain, max_depth
        self.update_interval = update_interval
        self.reached_limit = reached_limit;

        self.workers_limit = 5
        self.worker_list, self.visited_url, self.url_queue, self.crawled = list(), set(), list(), {}
        self.url_queue.append("http://{}".format(seed_page))
        self.seed_domain = get_domain_name(re.sub('https?://', '', seed_page))
        if '/' in self.seed_domain :
            self.seed_domain = self.seed_domain.split('/')[0]
        self.stats = {0 : {self.seed_domain : 0}}
        for i in range(1, max_depth+1):
            self.stats[i] = {}
        self.debug, self.verbose = debug, verbose
        if os.path.isfile("./crawled_documents"): #Already crawled
            self.reached_limit.value = 1
            self.documents_indexed.value = False
            return
        try:
            flush_file = open('crawled_documents', 'w+', encoding='utf-8')
            text_index = open('text_index', 'w+', encoding="utf-8")
            text_index.write("")
            flush_file.write("")
            text_index.close()
            flush_file.close()
        except:
            self.print_v("Failed to create new flush file")
        self.crawlLoop()



    def crawlLoop(self):
        stop_limit = 500 
        flushCount = 0
        while True: #Infinite loop
            idle = True
            while len(self.url_queue) > 0 and self.workers_limit - len(self.worker_list) > 0: #Load the workers
                idle = False
                urlToLoad = self.url_queue.pop()
                self.visited_url.add(urlToLoad)
                self.print_v("Requesting URL : {}".format(urlToLoad))
                self.worker_list.append(fetcher.HttpAsyncRequest(urlToLoad))
                self.worker_list[-1].send()  

            for w in self.worker_list: #Check if some workers finished
                if w.is_complete():# and w.get_response_code() in [200]:
                    crawled_time = int(time.time())
                    self.print_d(w.get_response_code())
                    if w.get_response_code() not in [200, 302]:
                        self.worker_list.remove(w)
                        continue
                    idle = False
                    #Load page in soup
                    #Handle redirection
                    if w.get_response_code() == 302:
                        w_url = w.get_response_header("Location")
                        self.print_d("Page redirected to {}".format(w_url))
                    else:
                        w_url = w.url
                    #Handle malformed redirection and url
                    if '?'in w.url:
                        w_url = w.url.split('?')[0]
                    if '@' in w.url:
                        w_url = w.url.split('@')[0]
                    current_url = format_url(w.scheme, w.host, w.path, w.url)
                    self.print_v("Successfully loaded URL : {}".format(current_url))
                    self.crawled[current_url] = {}
                    soup = BeautifulSoup(w.get_response_content(mode=''), 'html.parser')
                    for s in soup(["script", "style"]): # remove all script and style content
                        s.extract()

                    #Extract outlinks
                    #print(f.info().get_content_charset())
                    outlinks = set()
                    extension_list = ['htm', 'html', 'xhtml', 'php', 'asp', 'aspx', 'pl', 'rb', 'rhtml', 'php4', 'php3', 
    '                       phtml', 'shtml', 'jhtml', 'jsp', 'jspx']
                    for a in soup.find_all('a', href=True):
                        if len(a['href']) > 2 and 'javascript:' not in a['href']:
                            extract_url = a['href']
                            if '?' in extract_url:
                                extract_url = extract_url[:extract_url.index('?')]
                            if len(extract_url) > 2 and '@' not in extract_url:
                                extract_url = extract_url.strip()
                                extract_url = format_url(w.scheme, w.host, w.path, extract_url)
                                extension = get_file_extension(extract_url)
                                if extension in extension_list or len(extension) == "":
                                    outlinks.add(extract_url)
                                    #print("Formatted URL : {}".format(extract_url))
                    worker_domain = get_domain_name(w.host)
                    self.crawled[current_url]['links'] = outlinks
                    #self.crawled[current_url]['domain'] = worker_domain
                    self.crawled[current_url]['time'] = worker_domain
                    self.print_v("Extracted the page's link")

                    depth = 0
                    for i in self.stats:
                        if worker_domain in self.stats[i]:
                            depth = i    
                    for l in outlinks: #Load new url in the queue
                        if l not in self.visited_url and l not in self.url_queue:
                            next_depth, added = depth + 1, False
                            hostname = urllib.parse.urlparse(l).hostname
                            if hostname is None:
                                continue
                            self.print_d(l)
                            next_domain = get_domain_name(urllib.parse.urlparse(l).hostname)
                            for i in self.stats:
                                if next_domain in self.stats[i]: #domain is in stats
                                    if self.stats[i][next_domain] < self.max_domain_pages: #domain has not max page
                                        self.url_queue.append(l)
                                        self.stats[i][next_domain] += 1
                                        self.print_d("Domain",next_domain,"depth",next_depth,";",self.stats[i][next_domain],"pages")
                                    added = True
                            if next_depth <= self.max_depth and not added: #domain is not in stats
                                if len(self.stats[next_depth]) < self.max_domain: #domain can be added
                                    self.url_queue.append(l)
                                    self.stats[next_depth][next_domain] = 1
                                    self.print_d("Domain",next_domain,"depth",next_depth,";",self.stats[next_depth][next_domain],"pages")
                    self.print_v("Found",len(outlinks),"URL Q:",len(self.url_queue),"Lv1:",len(self.stats[1]),"Lv2:",len(self.stats[2]))

                    #Extract text
                    text = soup.get_text().replace('\n',' ').replace('\r',' ')
                    words = ' '.join([w for w in text.split(' ') if w])
                    self.crawled[current_url]['text'] = words
                    self.print_v('Extracted page text')
                    self.worker_list.remove(w)

                    
                    flushCount += 1
            #Handle results flushing
            if flushCount > 0:
                if flushCount >= stop_limit or (len(self.url_queue) == 0 and len(self.worker_list) == 0):
                    self.print_v("Writing last",flushCount,"crawled results to disk")
                    if not self.documents_lock.value:
                        self.documents_lock.value = True
                        try:
                            flush_file = open('crawled_documents', 'a+', encoding='utf-8')
                            text_index = open('text_index', 'a+', encoding="utf-8")                     
                            for u in self.crawled:
                                c = self.crawled[u]
                                links = '?'.join(c['links'])
                                header_data = [str(len(field)) for field in [u, c['time'], c['text'], links]]
                                header_head = ''.join([str(len(str(h))) for h in header_data])
                                data = ''.join( [u, str(c['time']), c['text'], links ])
                                offset = flush_file.tell() 
                                text_index.write("{}?{}\n".format(u, offset))
                                flush_file.write("{}{}{}\n".format(header_head, ''.join(header_data), data))
                            flush_file.close()
                            text_index.close()   
                            self.crawled = {}
                            flushCount = 0      
                            self.print_v("Successfully wrote crawling results")
                            self.documents_indexed.value = False
                        except:
                            print("Error while writing the crawling results")
                            PrintException()
                        self.documents_lock.value = False                
                    
            if idle:
                if len(self.worker_list) == 0:
                    if len(self.url_queue) == 0:
                        self.reached_limit.value = True
                    time.sleep(10)
                else:
                    time.sleep(1) #Wait a little for things to change
            self.print_v("Looping... {} URL in queue, {} workers working".format(len(self.url_queue), len(self.worker_list)))


def Crawler(reached_limit, documents_lock, documents_indexed, documents_folder, max_domain_pages, max_domain, max_depth, update_interval, seed_page, debug=False, verbose=True):
    crawler_instance = CrawlerClass(reached_limit, documents_lock, documents_indexed, documents_folder, max_domain_pages, max_domain, max_depth, update_interval, seed_page, debug, verbose)

    

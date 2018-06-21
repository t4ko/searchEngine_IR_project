# searchEngine_IR_project
Rough search engine developped as a final project for an Information Retrieval class.
The project has also been used for a performance analysis class which explain some timing instructions.

This project is a good example of "reinvent the wheel but make it octogonal" since the goal was to understand and practice the basics of information retrieval. No databases were used and the data storage is very inefficient since it was never intended to gather a realistic load for a web search engine.

This web search ranks the result for a given query in 2 steps:
- Compute a BM25 score and select the top 20 results
- Rerank the results by PageRank scores and select the top 10 results. Only 10 results are given to a query to easily assess relevance of a document to a query.

The relevance scores were assigned manually in 3 level :
- 0: no word from the query (should not happen)
- 1: contain words from the query but is irrelevant
- 2: words from the query and relevant content


Usage :
Tune the parameters in searchEngine.py to adjust the crawling
To start the search engine just run searchEngine.py


Limitations :
- The structure of this program is strange since it was initially designed to be realistic and able to run as a pipe for information but it was limited to match the needs of a class final project. The modules (crawler, indexer and query engine) run in separate processes started from searchEngine. It could in theory crawl documents continuously and index them in batches.
- By default 5 workers are used to download the pages and there is no notion of domain or politeness.
- Only one worker can parse its downloaded page at a given time, this can cause long hang times when the encoding are difficult to figure.
- Python may not be the best fit for the indexer and the query engine if it were to become bigger since it's heavy on computing power.
- There is no scraper implemented to extract the content from a page

Easy evolutions :
- Use a real database
- Use scrapy to make a proper crawler/scraper
- Add a human friendly interface, typically a web page

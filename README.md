## Basic python based recursive webscraper

[![pipeline status](https://bear.cira.colostate.edu/abrammer/web_scraper/badges/master/pipeline.svg)](https://bear.cira.colostate.edu/abrammer/web_scraper/commits/master)
[![coverage report](https://bear.cira.colostate.edu/abrammer/web_scraper/badges/master/coverage.svg)](https://bear.cira.colostate.edu/abrammer/web_scraper/commits/master)


Simple python utility to scrape a http or ftp locations and recursively sync remote files/diorectories.  
Will traverse subdirectory hyperlinks for html pages. 
Modified time from the server is checked through a head requests and only new files are downloaded.  
Times are synced locally to match the remote system and downloads are spread across multiple threads to speed up the whole process. 

Local files are not removed if they no longer exist on the remote server, so syncing remote rolling archvies (e.g. realtime nomads) is easy.  

Could probably be replicated in a single wget command, but now we have concurrency and can utilise as a function to trigger other actions.  


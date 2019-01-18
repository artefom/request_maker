request-maker
=================

Tool for making scheduled parallel requests.


Installation
------------

No installation needed


Basic Usage
-----------


To run scraping from websites, run:
```
python scheduler.py read_websites inputs/websites_total.txt outputs/websites --dump-every 10000 --request-per-second 10 --n-threads 30 --duration-hours 9
```

read_websites - name of the script to run
inputs/websites_total.txt - input file (each new line will be passed to read_websites.main() )
outputs/websites - output destination

for more info ```python scheduler.py -h```

read_websites can be changed to any arbitrary function with any result. Inspect read_websites.py for more info

scheduler.py runs run_scraping.py in multiple threads, which, in turn, runs scrape_scripts/read_websites.py that is passed as an argument "read_websites"

results are written to outputs/websites/api_call_cache_YYYMMDD_N.pkl as python dictionaries with the following format:

```
{
	(website1,) : ( .main() result, start_time, end_time, exception ),
	(website2,) : ( .main() result, start_time, end_time, exception )
	(website3,) : ( .main() result, start_time, end_time, exception )
}
```
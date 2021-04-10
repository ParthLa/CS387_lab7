import requests
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession, Row


import re

base_url = "https://hari1500.github.io/CS387-lab7-crawler-website"

def dataset(url):
	url = base_url+url
	h = requests.head(url)
	header = h.headers
	content_type = header.get('content-type')
	if content_type=="text/html; charset=utf-8":
		request_text = requests.get(url)
		parse_text = request_text.text
		res = re.findall('<a\s+(?:[^>]*?\s+)?href="([^http?"][^"]*)"',parse_text)
		return ["/"+x for x in res]
	

   
if __name__ == "__main__":
    # create Spark context with necessary configuration
	sc = SparkContext("local", "Web Crawler")
	spark = SparkSession(sc)
	rdd = sc.parallelize(dataset("/1.html"))
	new_ones = rdd
	rdd = rdd.union(sc.parallelize(["/1.html"]))
	# while new_ones!=None and len(new_ones.take(1))>0:
	while new_ones.isEmpty()==False:#len(new_ones.head(1)) != 0 :#new_ones.isEmpty()==0: #new_ones.take(1).length!=0:
		
		res = new_ones.flatMap(dataset)
		new_ones = res.subtract(rdd).distinct()
		new_ones.cache()
		rdd = rdd.union(res)
		rdd.cache()

	final = rdd.map(lambda row: 1).reduceByKey(lambda x,y: x+ y)
	rdd.map(Row("link")).toDF().show(300)

	final.saveAsTextFile('./webcrawler/')


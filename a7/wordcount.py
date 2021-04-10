from pyspark.sql import SQLContext
from pyspark import SparkContext
import re
# other required imports here
 
def process(line):
    # fill
    # convert all characters into lower case
    # print(line)
    line0=line["date_published"].split("T")[0]
    print(line0)
    line1=line["article_body"].lower()
    print(line1)
    # replace all non-alphanumerics with whitespace
    line1=re.sub('[^0-9a-zA-Z]',' ',line1)
    # split on whitespaces
    line1=line1.split(" ")
    l=[]
    for word in line1:
    	w=line0+" "+word
    	l.append(w)
    # return list of words
    return l
   
if __name__ == "__main__":
    # create Spark context with necessary configuration
    spark = SparkContext("local", "Word Count")

    # read json data from the newdata directory
    df = SQLContext(spark).read.option("multiLine", True) \
    .option("mode", "PERMISSIVE").json("./newsdata")

    # split each line into words
    lines = df.select("date_published", "article_body").rdd
    print(lines)
    words = lines.flatMap(process)

    # count the occurrence of each word
    wordCounts = words.map(lambda row: (row,1)).reduceByKey(lambda x,y: x+ y)

    # save the counts to output
    wordCounts.saveAsTextFile("./wordcount/")

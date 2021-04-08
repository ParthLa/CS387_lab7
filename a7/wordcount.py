from pyspark.sql import SQLContext
from pyspark import SparkContext
import re
# other required imports here
 
def process(line):
    # fill
    # convert all characters into lower case
    print(line)
    line=line.lower()
    # replace all non-alphanumerics with whitespace
    line=re.sub('[^0-9a-zA-Z]',' ',line)
    # split on whitespaces
    line=line.split(" ")
    # return list of words
    return line
   
if __name__ == "__main__":
    # create Spark context with necessary configuration
    spark = SparkContext("local", "Word Count")

    # read json data from the newdata directory
    df = SQLContext(spark).read.option("multiLine", True) \
    .option("mode", "PERMISSIVE").json("./newsdata")

    # split each line into words
    lines = df.select("date_published", "article_body").rdd
    words = lines.flatMap(process)

    # count the occurrence of each word
    wordCounts = words.map(lambda row: 1).reduceByKey(lambda x,y: x+ y)

    # save the counts to output
    wordCounts.saveAsTextFile("./wordcount/")

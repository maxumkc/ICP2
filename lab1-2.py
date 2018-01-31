import sys
import os

from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":

    conf = SparkConf().setMaster("local")
    sc = SparkContext(conf=conf)

    words = sc.textFile("class2.data").flatMap(lambda x: x.split(" "))
    group_words = words.groupBy(lambda x: x[0])


    print([(w[0], [i for i in list(w[1])]) for w in group_words.collect()])

    output = [(w[0], [i for i in w[1]]) for w in group_words.collect()]
    file = open('output.txt', 'w')
    file.write("\n".join(str(x) for x in output))
    file.close()
    sc.stop()


import sys
import os

from pyspark import SparkContext
from pyspark import SparkConf

 # define an interation                         #[0]     [1]
def transform(tuple): # tuple = (U, {United, Unified})
    r = tuple[0]  # r = U
    for item in tuple[1]:
        r += ", " + item #iteration1: r = U, United
                        #iteration2: r = U, United, Unified
    return r

conf = SparkConf().setMaster("local")
sc = SparkContext(conf=conf)

#read a file
lines=sc.textFile("class2.data")
# United States Incident Separated Unified Investments Board

# split the line by space into words
words = lines.flatMap(lambda line: line.split(' '))
# [ United, States, ..., Board]

# map each word with its first letter as key. ex: ('U', 'United')
uUnited = words.map(lambda word: (word[0], word))
# [ (U, United), (S, States), (U, Unified), ..., (B, Board)

# group by the key and transform the uUnited into str
uGroup = uUnited.groupByKey().map(transform)
# [ (U, {United, Unified}),   (S, {States, Separated}), .... ]

#uGroup = [ "U, United, Unified",   "S, States, Separated", .... ]

#saving the output file
uGroup.saveAsTextFile("laboutput.txt")

#print a output
for var in uGroup.collect():
    print(var)
/root/PycharmProjects/lab1/lab1.py
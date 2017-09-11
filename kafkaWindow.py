# -*- coding: utf-8 -*-
"""
Created on Wed Sep 06 23:34:10 2017

@author: yoga.Phani
"""
from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka  import KafkaUtils
#from pyspark.sql import SQLContext

def createContext():
    print("Creating new context")
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)
    #sqlContext = SQLContext(sc)
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    jsonstream=kvs.map(lambda x: json.loads(x[1])).map(lambda y : (y['netid'],y['src_sys']))
    jsonstream.pprint()
    jsonstream.countByValue().transform(lambda rdd:rdd.sortBy(lambda x:-x[1])).map(lambda x:"counts this batch:\tValue %s\tCount %s" % (x[0],x[1])).pprint()
    jsonstream.countByValueAndWindow(60,30).transform(lambda rdd:rdd.sortBy(lambda x:-x[1])).map(lambda x:"Window counts this batch:\tValue %s\tCount %s" % (x[0],x[1])).pprint()
    
    return ssc

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: window.py <topic>", file=sys.stderr)
    checkpoint='/hdfsproc/pyspark_checkpoint'
    ssc = StreamingContext.getOrCreate(checkpoint,lambda: createContext())
    ssc.start()
    ssc.awaitTermination()

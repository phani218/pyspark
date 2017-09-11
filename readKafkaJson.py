# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 09:55:13 2017

@author: Yoga.Phani
"""
from __future__ import print_function

import sys
import json


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SQLContext



def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']




def process(rdd):

    sqlContext = getSqlContextInstance(rdd.context)
    if rdd.count() >0 :
        rowRdd=rdd.map(lambda y: Row(src_sys=y['src_sys'], netid=y['netid'], src_uid=y['src_uid'], telephone_num=y['telephone_num'],telephone_type=y['telephone_type'], type_desc=y['type_desc'],src_ts=y['src_ts']))
        jsonDataFrame = sqlContext.createDataFrame(rowRdd)
        jsonDataFrame.show()
        jsonDataFrame.printSchema()
        print("Writing to Telephone Table")
        jsonDataFrame.write.format("org.apache.spark.sql.cassandra").mode("append").options(table= "telephone_num",
                                                                                            keyspace="dev_datalake").save();
        print("Writing to Telephone Log Table")
        jsonDataFrame.write.format("org.apache.spark.sql.cassandra").mode("append").options(table="log_telephone_num",
                                                                                            keyspace="dev_datalake").save();
    else :
        print("RDD is EMPTY")

def createContext():
    print("Creating new context")
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('hdfs:///hdfsproc/pyspark_checkpoint_2')
    brokers = sys.argv[1]
    topic = sys.argv[2]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    #jsonstream = kvs.map(lambda x: json.loads(x[1])).map(lambda y: Row(src_sys=y['src_sys'], netid=y['netid'], src_uid=y['src_uid'], telephone_num=y['telephone_num']))
    jsonstream = kvs.map(lambda x: json.loads(x[1]))

    jsonstream.pprint()
    jsonstream.foreachRDD(process)

    #jsonDataFrame = sqlContext.createDataFrame(jsonstream)
    ##jsonDataFrame.show
    return ssc


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: window.py <topic>", file=sys.stderr)
    checkpoint = 'hdfs:///hdfsproc/pyspark_checkpoint_2'
    ssc = StreamingContext.getOrCreate(checkpoint, lambda: createContext())
    #ssc=StreamingContext.getOrCreate(checkpoint,setupFunc=createContext())
    ssc.start()
    ssc.awaitTermination()

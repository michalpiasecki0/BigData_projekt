import happybase
import pandas as pd
import os
import findspark
import copy
import sys
import subprocess

from datetime import datetime
from typing import List
from functools import reduce
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark import SparkContext, SparkConf
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, concat
from pathlib import Path


findspark.init()


def load_tables_younger_that(date: datetime, file_paths: list) -> list:
    """
    Load tables, which are younger than some date, having list of filepaths to hdfs
    """
    def get_file_path_timestamp(file_path: str):
        return float(file_path.split('reddit')[1].split('.')[0])    
   
    assert isinstance(date, datetime)    

    date_timestamp = date.timestamp()
    reddit_tables = []
    
    for file_path in file_paths:
        if get_file_path_timestamp(file_path) > date_timestamp:
            reddit_tables.append((spark.read.format('parquet').
                  option('sep', '|').
                  option('header', True).
                  option('inferSchema', False).                
                  load(file_path)))
    return reddit_tables


if __name__ == '__main__':
    
    if len(sys.argv) != 1:
        year, month, day = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
        
    # finding hdfs paths
    bashCommand = "hdfs dfs -ls /user/projekt/nifi_out"
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output_paths, _ = process.communicate()
    output_paths = output_paths.decode('utf-8').split('\n')
    print(output_paths)

    # getting convenient paths to hdfs
    files = []
    for file_path in output_paths:
        if file_path.find('reddit') != -1:
            files.append('hdfs://localhost:8020/' + file_path.split(sep=' ')[-1])

    print(files)
    # starting spark
    spark = (
        SparkSession.builder
        .appName("DATA LOADER FROM HDFS")
        .getOrCreate()
        )

    # loading and concatenating tables from hdfs
   
    reddit_tables = []
    for file in files:
        reddit_tables.append((spark.read.format('parquet').
                      option('sep', '|').
                      option('header', True).
                      option('inferSchema', False).                
                      load(file)))
        reddit_tables[-1] = reddit_tables[-1].drop('id').drop('ups').drop('downs').drop('upvote_ratio').drop('wls')
        reddit_tables[-1] = reddit_tables[-1].withColumn('id', concat(reddit_tables[-1].author_fullname,                       reddit_tables[-1].created))


    combined = reduce(DataFrame.unionAll, reddit_tables)
    
    w2 = Window.partitionBy("id").orderBy(col("created").desc())
    combined = combined.withColumn("row",row_number().over(w2)) \
                     .filter(col("row") == 1).drop("row")
    
    combined.write.mode('overwrite').parquet('file:///home/vagrant/projekt/data/reddit.parquet')
    
    # happybase
    
    connection = happybase.Connection('localhost')
    
    if b'reddit_hbase' not in connection.tables():
        connection.create_table(
        'reddit_hbase',
    {
        'text_data': dict(), # text, title
        'general_info': dict(), # id, created, kind 
        'evaluation': dict(), # downs, num_comments, ups, score
    })    
    
    hbase_table = connection.table("reddit_hbase")
    rows = combined.collect()
        
    for i, r in enumerate(rows):
        hbase_table.put(str(r['id']), {b'general_info:created': str(r['created']),
                                       b'general_info:kind': str(r['kind']),
                                       b'text_data:title': str(r['title']),
                                       b'text_data:text': str(r['text']),
                                       b'evaluation:num_comments': str(r['num_comments']),
                                       b'evaluation:score': str(r['score'])                             
                                  })
    # check if this nightmare works
    for key, data in hbase_table.scan():
        print(key, data)
        break
    '''
    if b'reddit_hbase' in connection.tables():
        # if there is already table, we delete it and create a new one
        connection.delete_table(b'reddit_hbase', disable=True)

    connection.create_table(
        'reddit_hbase',
    {
        'text_data': dict(), # text, title
        'general_info': dict(), # id, created, kind 
        'evaluation': dict(), # downs, num_comments, ups, score
    })
    hbase_table = connection.table("reddit_hbase")
    '''

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
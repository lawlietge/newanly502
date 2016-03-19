#!/usr/bin/spark-submit
#
# Problem Set #4
# Implement wordcount on the shakespeare plays as a spark program that:
# a.Removes characters that are not letters, numbers or spaces from each input line.
# b.Converts the text to lowercase.
# c.Splits the text into words.
# d.Reports the 40 most common words, with the most common first.

# Note:
# You'll have better luck debugging this with ipyspark

import sys
from operator import add
from pyspark import SparkContext


import matplotlib as plt
from numpy import *
import pandas 
from matplotlib import pyplot
import datetime



if __name__ == "__main__":
    
    ##
    ## Parse the arguments
    ##

    infile =  's3://gu-anly502/ps03/freebase-wex-2009-01-12-articles.tsv'

    ## 
    ## Run WordCount on Spark
    ##

    sc     = SparkContext( appName="Wikipedia Count" )
    lines  = sc.textFile( infile )

    counts = lines.map(lambda date: date.split('\t')[2]) \
                  .map(lambda month:month[0:7])  \
		  .map(lambda x: (x,1))  \
                  .reduceByKey(add)
    month_sorted = counts.sortBy(lambda x: x[0]).collect()


    ## YOUR CODE GOES HERE
    ## PUT YOUR RESULTS IN counts


    with open("wikipedia_by_month.txt","w") as fout:
        for (date, count) in month_sorted:
            fout.write("{}\t{}\n".format(date,count))
    
    ## 
    ## Terminate the Spark job
    ##
    #############
    # plot
    #############
    data = pandas.read_csv("wikipedia_by_month.txt", sep='\t', header = None, names=['date', 'count'])
    fig = pyplot.figure()
    index = np.arange(len(data[0]))
    pyplot.bar(index, data['count'], width = width)
    fig.autofmt_xdate()
    plt.xticks(index, data['date'], rotation = 'vertical')
    plt.savefig("figure.pdf")
    
    sc.stop()

import matplotlib
from numpy import *
import pandas 
from matplotlib import pyplot
import datetime    



Data = pandas.read_csv("wikipedia_by_month.txt", sep='\t', header = None, names=['date', 'count'])
    fig = pyplot.figure()
    index = np.arange(len(Data[0]))
    pyplot.bar(index, Data['count'], width = width)
    fig.autofmt_xdate()
    plt.xticks(index, Data['date'], rotation = 'vertical')
    plt.savefig("figure.pdf") 

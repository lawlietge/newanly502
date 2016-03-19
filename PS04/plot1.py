import matplotlib as plt
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import datetime    


Data = pd.read_csv("wikipedia_by_month.txt", sep='\t', header = None, names=['Date', 'Count'])

fig = plt.figure()
ind = np.arange(len(Data['Date']))
plt.bar(ind, Data['Count'])
fig.autofmt_xdate()
plt.xticks(ind, Data['Date'], rotation = 'vertical')
plt.savefig("figure.pdf")

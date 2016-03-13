#!/usr/bin/env python2.7

# Lets read the file

import csv

if __name__=="__main__":
    with open("presidents.csv","r") as f:
        rows = []
        cols = f.readline().strip().split(',') # get the named cols
        for line in f:
            # Read a row and turn it into a dictionary
            row = dict(zip(cols, [a.strip() for a in csv.reader([line]).next()]))
            rows.append(row)
    
    presidentsTable = sqlCtx.createDataFrame(rows)
    presidentsTable.registerTempTable("pres")
    print(sqlCtx.sql("select name,party from pres").collect())


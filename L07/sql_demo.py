#!/usr/bin/env python2.7

# Lets read the file

import csv

if __name__=="__main__":
    with open("presidents.csv","r") as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [row for row in reader]

    
    presidentsTable = sqlCtx.createDataFrame(rows)
    presidentsTable.registerTempTable("pres")
    print(sqlCtx.sql("select name,party from pres").collect())


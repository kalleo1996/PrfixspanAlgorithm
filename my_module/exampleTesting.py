from pyspark.ml.fpm import PrefixSpan
from pyspark.shell import sc
from pyspark.sql.functions import desc
from pyspark.sql.types import Row

from csv import reader

# open file in read mode
list = []

with open("D:\A-4LS1\A-BigData\store_data2.csv", 'r') as read_obj:
    # pass the file object to reader() to get the reader object
    csv_reader = reader(read_obj)
    customerlist=[]
    sequence = []
    anotherList=[]
    for row in csv_reader:# row variable is a list that represents a row in csv  # print(row)

        list2 = []

        for x in row:

              if x.isdigit():
                print("before")
                print(x)
                if  len(customerlist) == 0 or  customerlist[len(customerlist) - 1] != x:

                     if len(customerlist) != 0:
                            list.append(Row(sequence=anotherList))

                     customerlist.append(x)
                     sequence=[]
                     anotherList=[]

              if x != "" and not x.isdigit() :

                  list2.append(x)

        anotherList.append(list2)
list.append(Row(sequence=anotherList))
print(list)



df = sc.parallelize(list).toDF()

prefixSpan = PrefixSpan(minSupport=0.1, maxPatternLength=3,
                        maxLocalProjDBSize=32000000)

# Find frequent sequential patterns.
prefixSpan.findFrequentSequentialPatterns(df).sort(desc("freq")).show(10,False)

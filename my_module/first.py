# # from pyspark import SparkContext, SparkConf
# #
# # if __name__ == "__main__":
# #
# #     # create Spark context with necessary configuration
# #     sc = SparkContext("local","PySpark Word Count Exmaple")
# #
# #     # read data from text file and split each line into words
# #     words = sc.textFile("input.txt").flatMap(lambda line: line.split(" "))
# #
# #     # count the occurrence of each word
# #     wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
# #
# #     # save the counts to output
# #     wordCounts.saveAsTextFile("output/")
# # from pyspark.ml.fpm import FPGrowth
# # from pyspark.shell import spark
# #
# #
# # df = spark.createDataFrame([
# #     (0, [1, 2, 5]),
# #     (1, [1, 2, 3, 5]),
# #     (2, [1, 2])
# # ], ["id", "items"])
# #
# # fpGrowth = FPGrowth(itemsCol="items", minSupport=0.5, minConfidence=0.6)
# # model = fpGrowth.fit(df)
# #
# # # Display frequent itemsets.
# # model.freqItemsets.show()
# #
# # # Display generated association rules.
# # model.associationRules.show()
# #
# # # transform examines the input items against all the association rules and summarize the
# # # consequents as prediction
# # model.transform(df).show()
# from pyspark import SparkContext
#
# # Spark Context
# # Also extra coniguration can be added
# sc = SparkContext("local" , "Apriori")
# file =  sc.textFile("fruits.csv")
#
# print(file.collect())
#
# # RETURNS :
# # ['Apple,Mango,Banana', 'Banana,Mango', 'Apple,Banana', 'Apple,Mango,Coconut',
# # 'Strawberry,Grapes,Lemon,Raspberry', 'Rassberry,Grapes', 'Strawberry,Apple',
# # 'Apple,Mango,Raspberry', 'Mango,Raspberry', 'Mango,Apple', 'Apple,Raspberry',
# # 'Banana,Raspberry,Mango', 'Apple,Mango,Banana', 'Raspberry,Banana',
# # 'Apple,Strawberry', 'Strawberry,Banana,Apple,Mango', 'Mango,Banana,Raspberry,Apple',
# # 'Coconut,Apple,Raspberry', 'Raspberry,Coconut,Banana']
#
# ## Splited items
# lblitems = file.map(lambda line: line.split(','))
#
# print(lblitems.collect())
#
# # RETURNS
# # [
# #    ['Apple', 'Mango', 'Banana'], ['Banana', 'Mango'], ['Apple', 'Banana'],
# #    ['Apple', 'Mango', 'Coconut'], ['Strawberry', 'Grapes', 'Lemon', 'Raspberry'],
# #    ['Rassberry', 'Grapes'], ['Strawberry', 'Apple'],
# #    ['Apple', 'Mango', 'Raspberry'], ['Mango', 'Raspberry'],
# #    ['Mango', 'Apple'], ['Apple', 'Raspberry'], ['Banana', 'Raspberry', 'Mango'],
# #    ['Apple', 'Mango', 'Banana'], ['Raspberry', 'Banana'], ['Apple', 'Strawberry'],
# #    ['Strawberry', 'Banana', 'Apple', 'Mango'], ['Mango', 'Banana', 'Raspberry', 'Apple'],
# #    ['Coconut', 'Apple', 'Raspberry'], ['Raspberry', 'Coconut', 'Banana']
# # ]
#
# ## Whole lines in single array
# wlitems = file.flatMap(lambda line:line.split(','))
#
# print(wlitems.collect())
#
# # RETURN :
#
# # ['Apple', 'Mango', 'Banana', 'Banana', 'Mango', 'Apple', 'Banana',
# #  'Apple', 'Mango', 'Coconut', 'Strawberry', 'Grapes', 'Lemon', 'Raspberry',
# #  'Rassberry', 'Grapes', 'Strawberry', 'Apple', 'Apple', 'Mango', 'Raspberry',
# #  'Mango', 'Raspberry', 'Mango', 'Apple', 'Apple', 'Raspberry', 'Banana',
# #  'Raspberry', 'Mango', 'Apple', 'Mango', 'Banana', 'Raspberry', 'Banana',
# #  'Apple', 'Strawberry', 'Strawberry', 'Banana', 'Apple', 'Mango', 'Mango',
# #  'Banana', 'Raspberry', 'Apple', 'Coconut', 'Apple', 'Raspberry',
# #  'Raspberry', 'Coconut', 'Banana']
#
# ## Unique frequent items in dataset
# uniqueItems = wlitems.distinct()
#
# # Add 1 as Tuple
# supportRdd = wlitems.map(lambda item: (item , 1))
#
# # Method for sum in reduceByKey method
# def sumOparator(x,y):
#     return x+y
#
# # Sum of values by key
# supportRdd = supportRdd.reduceByKey(sumOparator)
#
# # print(supportRdd.collect()) # Retruns following array
# # [('Apple', 12), ('Mango', 10), ('Banana', 9), ('Coconut', 3),
# #  ('Strawberry', 4), ('Grapes', 2), ('Lemon', 1), ('Raspberry', 9), ('Rassberry', 1)]
#
#
# # First support values
# supports = supportRdd.map(lambda item: item[1]) # Return only support values
#
# # Define minimum support value
# minSupport = supports.min()
#
# # If mininmum support is 1 then replace it with 2
# minSupport = 2 if minSupport == 1 else minSupport
#
# ## Filter first supportRdd with minimum support
# supportRdd = supportRdd.filter(lambda item: item[1] >= minSupport )
#
# ## Craete base RDD with will be updated every iteration
# baseRdd = supportRdd.map(lambda item: ([item[0]] , item[1]))
# print('1 . Table has crated...')
#
# supportRdd = supportRdd.map(lambda item: item[0])
# supportRddCart = supportRdd
#
# def removeReplica(record):
#
#     if(isinstance(record[0], tuple)):
#         x1 = record[0]
#         x2 = record[1]
#     else:
#         x1 = [record[0]]
#         x2 = record[1]
#
#     if(any(x == x2 for x in x1) == False):
#         a = list(x1)
#         a.append(x2)
#         a.sort()
#         result = tuple(a)
#         return result
#     else:
#         return x1
#
#
# c = 2 # Combination length
#
# while(supportRdd.isEmpty() == False):
#
#     combined = supportRdd.cartesian(uniqueItems)
#     combined = combined.map(lambda item: removeReplica(item))
#
#     combined = combined.filter(lambda item: len(item) == c)
#     combined = combined.distinct()
#
#
#     combined_2 = combined.cartesian(lblitems)
#     combined_2 = combined_2.filter(lambda item: all(x in item[1] for x in item[0]))
#
#     combined_2 = combined_2.map(lambda item: item[0])
#     combined_2 = combined_2.map(lambda item: (item , 1))
#     combined_2 = combined_2.reduceByKey(sumOparator)
#     combined_2 = combined_2.filter(lambda item: item[1] >= minSupport)
#
#     baseRdd = baseRdd.union(combined_2)
#
#     combined_2 = combined_2.map(lambda item: item[0])
#     supportRdd = combined_2
#     print(c ,'. Table has crated... ')
#     c = c+1
#
# class Filter():
#
#     def __init__(self):
#
#         self.stages = 1
#
#
#     def filterForConf(self, item , total):
#
#         if(len(item[0][0]) > len(item[1][0])  ):
#             if(self.checkItemSets(item[0][0] , item[1][0]) == False):
#                 pass
#             else:
#                 return (item)
#         else:
#             pass
#         self.stages = self.stages + 1
#
#     # Check Items sets includes at least one comman item // Example command: # any(l == k for k in z for l in x )
#     def checkItemSets(self, item_1 , item_2):
#
#         if(len(item_1) > len(item_2)):
#             return all(any(k == l for k in item_1 ) for l in item_2)
#         else:
#             return all(any(k == l for k in item_2 ) for l in item_1)
#
#
#     def calculateConfidence(self, item):
#
#         # Parent item list
#         parent = set(item[0][0])
#
#         # Child item list
#         if(isinstance(item[1][0] , str)):
#             child  = set([item[1][0]])
#         else:
#             child  = set(item[1][0])
#         # Parent and Child support values
#         parentSupport = item[0][1]
#         childSupport = item[1][1]
#         # Finds the item set confidence is going to be found
#
#         support = (parentSupport / childSupport)*100
#
#         return list([ list(child) ,  list(parent.difference(child)) , support ])
#
#
# # Example ((('x10', 'x3', 'x6', 'x7', 'x9'), 1), (('x10', 'x3', 'x7'), 1))
# calcuItems = baseRdd.cartesian(baseRdd)
#
# # Create Filter Object
# ff = Filter()
#
# #deneme = calcuItems.map(lambda item: lens(item))
# total = calcuItems.count()
#
# print('# : Aggregated support values preparing for the confidence calculatations')
# baseRddConfidence = calcuItems.filter(lambda item: ff.filterForConf(item , total))
# print('# : Aggregated support values are ready !')
# baseRddConfidence = baseRddConfidence.map(lambda item: ff.calculateConfidence(item))
#
#
# print(baseRddConfidence.collect())

from pyspark.ml.fpm import PrefixSpan
from pyspark.shell import sc
from pyspark.sql.functions import desc
from pyspark.sql.types import Row

from csv import reader

# open file in read mode
list =[]

with open("D:\MyDownloads\store_data.csv", 'r') as read_obj:
    # pass the file object to reader() to get the reader object
    csv_reader = reader(read_obj)
    # Iterate over each row in the csv using reader object

    for row in csv_reader:
        # row variable is a list that represents a row in csv
        # print(row)
        sequence=[]
        list2=[]

        i=0
        for x in row:
                  eleme=[]
                  eleme.append(x)
                  #print(x)
                  list2.append(eleme)
                  #print(list2)

        list.append(Row(sequence=list2))


print(list)
df = sc.parallelize(list).toDF()

prefixSpan = PrefixSpan(minSupport=0, maxPatternLength=20,
                        maxLocalProjDBSize=32000000)

# Find frequent sequential patterns.
prefixSpan.findFrequentSequentialPatterns(df).sort("sequence").sort(desc("freq")).show(200,False)
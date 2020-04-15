# -*- coding: utf-8 -*-
"""

	CSE 547 - HW1 - Q1

	By Samarth Modi

"""


#Setup
!pip install pyspark
!pip install -U -q PyDrive
!apt install openjdk-8-jdk-headless -qq
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# Importing the necessary libraries
import pandas as pd
import numpy as np
from itertools import combinations
# %matplotlib inline

import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

# create the Spark session
conf = SparkConf().setAll([("spark.ui.port", "4050"), ("spark.executor.memory", '8g')])

# create the Spark context
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()


""" 

THE MAIN CODE STARTS HERE

"""

# Load data to a rdd
text = sc.textFile("soc-LiveJournal1Adj.txt")
rdd = text.map(lambda x: (int(x.split()[0]), [int(i) for i in x.split()[-1].split(',') ]))

# Get all users
all_rdd = rdd.map(lambda x: x[0])

# Tag first degree friends as 0
def func1(x):
  ans = []
  for i in x[-1]:
    ans.append(((x[0], i), 0))
  return ans

rdd2 = rdd.flatMap(lambda x: func1(x))

# Tag second degree friends as 1 (Two way combination)
def func2(x):
  comb = list(combinations(x[-1], 2))
  ans = []
  for c in comb:
    ans.append(((c[0], c[-1]), 1))
    ans.append(((c[-1], c[0]), 1))
  return ans

rdd3 = rdd.flatMap(lambda x: func2(x))

# Appending first degree and second degree tagged users
rdd4 = rdd2.union(rdd3)

# Add up only second degree friendship connections. Return 0 if we see a first degree friendship
def func3(a, b):

  if a==0 or b==0:
    return 0
  else:
    return a+b

reduced_rdd = rdd4.reduceByKey(lambda a, b: func3(a,b)).filter(lambda x: x[-1] > 0 )

# Encapsulate friend with the count of mutual friends
cleaned_rdd = reduced_rdd.map(lambda x: (x[0][0], (x[0][-1], x[-1])))

# Create a rdd for all those users who do not receive any recommendation based on the absence of users with mutual friends
to_add = set(all_rdd.collect()) - set(sorted_rdd.map(lambda x: x[0]).collect())
to_add_rdd = sc.parallelize(to_add).map(lambda x: (x, ([], 0)))

# Appending untagged users with the tagged ones
big_rdd = cleaned_rdd.union(to_add_rdd)

# Creating a DataFrame to perform aggregation
df = spark.createDataFrame(big_rdd, ["User", "Recommendation"])

# Aggregating the second degree friends for each user, and sorting the users
grouped_df = df.groupBy('User').agg(collect_list("Recommendation").alias("Reco")).sort(asc("User"))

# Creating a Pandas DataFrame to perform some operations
pandas_df = grouped_df.toPandas()

# Cleaning the recommendation list by retaining only the top 10 Recommendations sorted on mutual friend count and user
def cleaningFunc(x):
  y = sorted(x, key=lambda y: (-y[1], y[0]))
  z = [i[0] for i in y if i[0] != None]
  l = len(x)
  if l > 10:
    return z[:10]
  else:
    return z

pandas_df["Recommendations"] = pandas_df["Reco"].apply(lambda x: cleaningFunc(x))

# Dropping useless column. Thus we obtain our final DataFrame
final_df = pandas_df.drop(["Reco"], axis = 1)
print(final_df[:12])

# Checking recommendations for the users asked in the question

print("Sanity check:: User 11: ",list(final_df[final_df["User"] == 11]["Recommendations"])[0], "\n")
print("User 924: ",list(final_df[final_df["User"] == 924]["Recommendations"])[0])
print("User 8941: ",list(final_df[final_df["User"] == 8941]["Recommendations"])[0])
print("User 8942: ",list(final_df[final_df["User"] == 8942]["Recommendations"])[0])
print("User 9019: ",list(final_df[final_df["User"] == 9019]["Recommendations"])[0])
print("User 9020: ",list(final_df[final_df["User"] == 9020]["Recommendations"])[0])
print("User 9021: ",list(final_df[final_df["User"] == 9021]["Recommendations"])[0])
print("User 9022: ",list(final_df[final_df["User"] == 9022]["Recommendations"])[0])
print("User 9990: ",list(final_df[final_df["User"] == 9990]["Recommendations"])[0])
print("User 9992: ",list(final_df[final_df["User"] == 9992]["Recommendations"])[0])
print("User 9993: ",list(final_df[final_df["User"] == 9993]["Recommendations"])[0])



"""
END OF CODE 

Thank you!

"""
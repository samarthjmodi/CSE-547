# -*- coding: utf-8 -*-
"""

CSE547 - HW1 - Q2
By Samarth Modi


#Section 2: Association Rules
---
"""

# Importing Libraries
import pandas as pd
from itertools import combinations, permutations
from collections import Counter
f = open("browsing.txt", "r")
text = f.read()
text3 = text.split()
text1 = text.split(" \n")
text2 = [i.strip().split(" ") for i in text1]

# Itemset of size 1
dic_L1 = Counter(text3)

L1_f = []

for k in dic_L1.keys():
  if dic_L1[k] >= 100:
    L1_f.append(k)

L1_set = set(L1_f)

# Itemset of size 2

# Creating all possible combinations of size 2 from frequent itemsets of size 1
L2_set = set(list(combinations(L1_f, 2)))
dic_L2 = {}
L2_f = []

# Creating all possible combinations of size 2 from each basket, and 
# checking if they are frequent or not
for i in text2:
  y = list(combinations(i, 2))
  z = [(k[1],k[0]) for k in y]
  y.extend(z)
  for j in y:
    if j in L2_set:
      if j in dic_L2:
        dic_L2[j] += 1
        if dic_L2[j] == 100:
          L2_f.append(j)
      else:
        dic_L2[j] = 1


L2_f_set = set(L2_f)

# Calculate confidence scores
dic_L2_conf = {}

for k in L2_f:
  dic_L2_conf[(k[0], k[1])] = dic_L2[k] / dic_L1[k[0]]
  dic_L2_conf[(k[1], k[0])] = dic_L2[k] / dic_L1[k[1]]

print(list(sorted(dic_L2_conf.items(), key = lambda kv:(kv[1], kv[0]), reverse= True)[:5]))


# Itemset of size 3

# Creating all possible combinations of size 3 from frequent itemsets of size 1
L3 = list(combinations(L1_f, 3))
L3_set = set(L3)

L3_clean = []

# Checking if the subset of size 2 of frequent itemsets of size 3 are frequent
for i in L3:
  z = list(combinations(i,2))
  if ((z[0][0], z[0][1]) in L2_f_set or (z[0][1], z[0][0]) in L2_f_set):
    if ((z[1][0], z[1][1]) in L2_f_set or (z[1][1], z[1][0]) in L2_f_set):
      if((z[2][0], z[2][1]) in L2_f_set or (z[2][1], z[2][0]) in L2_f_set):
        L3_clean.append(i)

L3_clean_set = set(L3_clean)

dic_L3 = {}
L3_f = []

# Creating all possible combinations of size 3 from each basket, and 
# checking if they are frequent or not
for i in text2:
  y = list(combinations(i, 3))
  for k in y:
    for j in list(permutations(k)):
      if j in L3_clean_set:
        if j in dic_L3:
          dic_L3[j] += 1
          if dic_L3[j] == 100:
            L3_f.append(j)
        else:
          dic_L3[j] = 1
        break


# Calculate confidence scores
dic_L3_conf = {}

for k in L3_f:
  dic_L3_conf[(k[0], k[1], k[2])] = dic_L3_conf[(k[1], k[0], k[2])] = dic_L3[k] / dic_L2[(k[0], k[1])]
  dic_L3_conf[(k[0], k[2], k[1])] = dic_L3_conf[(k[2], k[0], k[1])] = dic_L3[k] / dic_L2[(k[0], k[2])]
  dic_L3_conf[(k[1], k[2], k[0])] = dic_L3_conf[(k[2], k[1], k[0])] = dic_L3[k] / dic_L2[(k[1], k[2])]


print(list(sorted(sorted(dic_L3_conf.items(), key = lambda x : (x[0][0], x[0][1])), key = lambda x : -x[1]))[:5])

"""

End of Code
Thank you!

"""


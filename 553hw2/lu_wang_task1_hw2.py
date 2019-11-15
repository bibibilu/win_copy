import time
import sys
from itertools import chain, combinations
from pyspark import SparkContext


sc = SparkContext("local", "pyspark")
caseNo = int(sys.argv[1])
support = int(sys.argv[2])
inputRDD = sc.textFile(sys.argv[3], 2)
# outputRDD = sc.textFile(sys.argv[4])

header = inputRDD.first()

if caseNo == 1:
    data = inputRDD.filter(lambda x: x != header).map(lambda line: line.split(",")).map(lambda x: ((x[0]), (x[1])))
elif caseNo == 2:
    data = inputRDD.filter(lambda x: x != header).map(lambda line: line.split(",")).map(lambda x: ((x[1]), (x[0])))

partitionNum = inputRDD.getNumPartitions()
threshold = support / 2

rdd2 = data.groupByKey().map(lambda x: set(x[1]))
# basketNum1 = len(rdd2.collect())
# basketNum2 = rdd2.Value().count()
# print(rdd2.collect())
# print(partitionNum)
# print(threshold)


# def fun_3(size):
#     candidates = []
#     if size == 2:
#         for candidate in combinationsfreq_items
#             can


def fun_1(baskets):
    freq_items = []
    count_1 = {}
    baskets = list(baskets)
    # result = []
    freq_itemset = {}
    size = 1
    # num_basket = 0
    for basket in baskets:
        for item in basket:
            # temp.append(item)
            if str(item) not in count_1.keys():
                count_1[str(item)] = 1
            else:
                count_1[str(item)] += 1
        # num_basket += 1
    for item in count_1:
        if count_1[item] >= threshold:
            freq_items.append(item)
    # freq_items = [{item} for item in c ]
    freq_items = sorted(freq_items)
    freq_itemset[size] = freq_items
    # frequent.append(temp)

    size = 2
    count_2 = {}
    candidate_pair = []
    candidate_set = list(set(combinations(freq_items, size)))
    for pair in candidate_set:
        for basket in baskets:
            if set(pair).issubset(basket):
                if tuple(pair) not in count_2:
                    count_2[tuple(pair)] = 1
                else:
                    count_2[tuple(pair)] += 1

    for pair in count_2:
        if count_2[pair] >= threshold:
            candidate_pair.append(pair)
    freq_items = sorted(candidate_pair)
    freq_itemset[size] = freq_items

    print("\nFreq_itemset:\n", freq_itemset)

    while len(freq_items) != 0:
        count_k = {}
        # freq_items = set([item for tp in freq_items for item in tp])
        candidate_set = [set(sorted(item)) for item in chain(*[combinations(freq_items, size)])]
        # newSet = set([])
        # for candidates in candidate_set:
        #     for sublist in candidates:
        #
        #             newSet.add(sublist)
        # candidate_set = list(newSet).copy()
        # print(freq_items)
        # print("\nCandidates:\n", candidate_set)
        # freq_items = []
        # temp = []
        for candidate in candidate_set:
            for basket in baskets:
                # sub_candidate = list(set(chain(*candidate)))
                # temp.append(sub_candidate)
                if set(candidate).issubset(basket):
                    if tuple(candidate) not in count_k:
                        count_k[tuple(candidate)] = 1
                    else:
                        count_k[tuple(candidate)] += 1
        # print("\ntemp:\n", temp)

        for candidate in count_k:
            if count_k[candidate] >= threshold:
                    # and set(sub_candidate).issubset(temp):
                # del temp[sub_candidate]
                freq_items.append(candidate)
        # if len(freq_items) == 0:
        #     break
        # freq_items = list(freq_items)
        # freq_items.sort()
        freq_itemset[size] = freq_items
        size += 1
    # print("\nfrequent itemset:\n", freq_itemset)

    return freq_itemset


# candidate_itemset = rdd2.mapPartitions(fun_1).reduceByKey(lambda x, y: 1).collect()
# .flatMap(lambda x: x)
candidate_itemset = rdd2.mapPartitions(fun_1).map(lambda x: (x, 1))
print(candidate_itemset.collect())


def fun_2(baskets):
    baskets = list(baskets)
    # candidate_itemset = []
    counts = {}
    for basket in baskets:
        for candidate in candidate_itemset:
            if set(candidate).issubset(basket):
                if tuple(candidate) not in counts:
                    counts[tuple(candidate)] = 1
                else:
                    counts[tuple(candidate)] += 1

    return counts.items()


# count_occurance = rdd2.mapPartitions(fun_2)
    # .reduceByKey(lambda x, y: x + y)
# final_freq = count_occurance.filter(lambda x: x[1] >= support).collect()
# print(count_occurance.collect())
# start = time.time()
#
# output_file = open(sys.argv[4], "w")
#
# print("candidate:\n", file=output_file)
#
# length_1 = {}
# for each in candidate_itemset:
#     if len(each) not in length_1:
#         length_1[len(each)] = [tuple(each)]
#     else:
#         length_1[len(each)] += [tuple(each)]
#
# for each in length_1:
#     length_1[each].sort()
#
# for key in length_1.keys():
#     v1 = length_1[key]
#     if key == 1:
#         print("("+str(v1[0][0])+")", file=output_file)
#     else:
#         print(str(v1[0]))
#     for i in range(1, len(v1)):
#         print(",", file=output_file)
#         print(str(v1[i]), file=output_file)
#     print("\n")
#
#
# print("Frequent Itemsets:\n", file=output_file)
#
# length_2 = {}
# for each in final_freq:
#     if len(each) not in length_2:
#         length_2[len(each)] = [tuple(each)]
#     else:
#         length_2[len(each)] += [tuple(each)]
#
# for each in length_2:
#     length_2[each].sort()
#
# for key in length_2.keys():
#     v2 = length_2[key]
#     if key == 1:
#         print("("+str(v2[0][0])+")", file=output_file)
#     else:
#         print(str(v2[0]))
#     for i in range(1, len(v2)):
#         print(",", file=output_file)
#         print(str(v2[i]), file=output_file)
#     print("\n")
#
#
# end = time.time()
#
# print("Duration: %s" % (end - start), file=output_file)
#
# output_file.close()

import json, sys
from typing import TextIO

from pyspark import SparkContext

sc = SparkContext("local[*]", "pyspark")
textRDD = sc.textFile(sys.argv[1]).cache()

A = textRDD.filter(lambda line: json.loads(line)["useful"] > 0).map(lambda line: json.loads(line)["useful"]).count()
B = textRDD.filter(lambda line: json.loads(line)["stars"] == 5).map(lambda line: json.loads(line)["stars"]).count()
C = textRDD.map(lambda line: (len(json.loads(line)["text"]), 1)).sortByKey(False).take(1)[0]
D = textRDD.map(lambda line: json.loads(line)["user_id"]).distinct().count()
E = textRDD.map(lambda line: (json.loads(line)["user_id"], 1)).reduceByKey(lambda a, b: a + b).sortBy(
    lambda line: line[1]
    , False).take(20)
F = textRDD.map(lambda line: json.loads(line)["business_id"]).distinct().count()
G = textRDD.map(lambda line: (json.loads(line)["business_id"], 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda line:
                                                                                                          line[1],
                                                                                                          False).take(
    20)

# with open(sys.argv[2], "w") as output_File:
#     data = json.load(output_File)
#     data.write("\"n_review_useful\":", A)
#     data.write("\t\"n_review_5_star\":", B)
#     data.write("\t\"n_characters\":", C)
#     data.write("\t\"n_user\":", D)
#     data.write("\t\"top20_user\":", E)
#     data.write("\t\"n_business\":", F)
#     data.write("\t\"top20_business\":", G)

# E1 = sc.textFile("./user.json").map(lambda line: line.split("\\^"))
# E2 = E1.map(lambda line: line[0], str(line[1]))
# E3 = sc.textFile("./review.json").map(lambda line: line.split("\\^"))
# E4 = E3.map(lambda line: line[1], 1).reducebykey(lambda a, b: a+b).distinct()
# E5 = E2.join(E4).distinct().collect()
# E6 = E5.sortby(False)
# D = E6.count()
# E = E6.take(20)

# G1 = sc.textFile("./business.json").map(lambda line: line.split("\\^"))
# G2 = G1.map(lambda line: line[0], str(line[1]))
# G3 = sc.textFile("./review.json").map(lambda line: line.split("\\^"))
# G4 = G3.map(lambda line: line[2], 1).reducebykey(lambda a, b: a+b).distinct()
# G5 = G2.join(G4).distinct().collect()
# G6 = G5.sortby(False)
# F = G6.count()
# G = G6.take(20)


import json, sys, time
from pyspark import SparkContext

sc = SparkContext("local[*]", "pyspark")
textRDD1 = sc.textFile(sys.argv[1]).cache()
textRDD2 = sc.textFile(sys.argv[2]).cache()

A1 = textRDD1.map(lambda line: (json.loads(line)["business_id"], (json.loads(line)["stars"], 1))).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
A2 = textRDD2.map(lambda line: (json.loads(line)["business_id"], json.loads(line)["state"]))
# A4 = textRDD2.map(lambda line: (json.loads(line)["state"], 1)).reduceByKey(lambda a, b: a+b)
A3 = A2.join(A1)
A4 = A3.map(lambda row: row[1]).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
A = A4.map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x: x[1], False)
method1 = A.collect()
method2 = A.take(5)

start_time1 = time.time()
print(method1[0], method1[1], method1[2], method1[3], method1[4])
end_time1 = time.time()
t1 = end_time1 - start_time1

start_time2 = time.time()
print(method2)
end_time2 = time.time()
t2 = end_time2 - start_time2

with open(sys.argv[3], "w") as output_file:
    data = json.load(output_file)
    data.write("\t\"state, stars\":")
    for p in data:
        data.write(method2)
        data.write("\t")
    data.write(method2)
    data.write("\t\"m1\":", t1)
    data.write("\t\"m2\":", t2)
    data.write("\t\"explanation\":", "")

import pandas as pd
import numpy as np
import csv
import json


def part1():
    data = pd.read_csv('movies.csv')
    df = pd.DataFrame(data)
    rate = df.groupby('movieID')['rating'].mean()
    newfile = rate.to_csv('part1.csv', header=False, sep=',')


def part2():
    data = pd.read_csv('movies.csv')
    df = pd.DataFrame(data)
    df.rename(columns={'rating': 'avg_rating'}, inplace=True)
    rate = df.groupby('movieID')['avg_rating'].mean()
    newfile = rate.to_csv('part2.csv', header=True, sep=',')
    -

def part3():
    metadata = []
    xfile = open("part2_metadata.csv", "w")
    metadata = [
        {
            "info": {
                'author: Lu Wang',
                'student_ID: 6568145789',
                'creation_time": "2019-02-05'
            },
        }
    ]

    xfile.write("\"author\": \"Lu Wang\""+"\n")
    xfile.write("\"student_ID\": \"6568145789\""+"\n")
    xfile.write("\"creation_date\": \"2019-02-05\""+"\n")
    xfile.close()
    file_metadata = pd.read_csv('part2_metadata.csv')

    print(file_metadata)


def write_csv_metadata():
    metadata = []
    file = pd.read_csv('part2.csv')
    file_metadata = pd.read_csv('part2_metadata.csv')
    file_metadata.to_csv('part3_extra.csv')
    con_file = pd.read_csv('part3_extra.csv')
    newfile = file.to_csv('part3_extra.csv', mode='a')

# def write_csv_metadata():


def part4():
    jsonfile = open("movies.json", "r")
    in_json = json.dumps(jsonfile)
    load_data = json.load(in_json)
    mother_list = []
    rating_list = []
    prior_id = 0
    dataset = load_data["data"]
    for item in dataset:
        movieid = item[1]-1
        rate = item[2]
        if prior_id != movieid:
            if len(mother_list) == 0:
                mother_list = [rating_list]
            else:
                mother_list.append(rating_list)
            prior_id = movieid
            rating_list = [rate]
        else:
            rating_list.append(rate)

    sum = []
    tmp = 0
    for i in range(0, len(mother_list)):
        for j in range(0, len(mother_list[i])):
            tmp += int(mother_list[i][j])
        sum.append(tmp)
        tmp = 0

    for i in range(0, len(mother_list)):
        movieid = i + 1
        sum[i] = sum[i] / len(mother_list[i])
        sum[i] = round(sum[i], 1)

    write_value = {}
    write_value["data"] = []
    for i in range(0, len(sum)):
        movieid = i+1
        tmp_list = [movieid, sum[i]]
        write_value["data"].append(tmp_list)

    info = {
        "info": {
             "author": "Lu Wang",
             "student_ID": "6568145789",
             "creation_time": "2019-02-05"
        },
    }
    print(load_data)
    load_data["metadata"].update(info)
    write_data = {}
    write_data["metadata"] = load_data["metadata"]
    write_data["data"] = (write_value["data"])
    # write_data["metadata"].update(info)
    # write_data.update(write_value)

    write_file = open("part4.json", "w")
    json.dump(write_data, write_file, sort_keys=False, indent=4)


def part5():
    meta = {
    "metadata": {
        "info": {
                "author": "Lu Wang",
                "student_ID": "6568145789",
                "cre ation_time": "2019-02-05"
                },
                "columns": {
                "userID": "description of the field",
                "avg_rating": "description of the field"
            }
        },
        "datafile": "part1.csv"
    }
    write_file = open("part5.json", "w")
    json.dump(meta, write_file, sort_keys=False, indent=4)


part1()
part2()
part3()
write_csv_metadata()
part4()
part5()

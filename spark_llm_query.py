# -*- coding: utf-8 -*-
# @Time    : 2025/7/4 17:15
# @Author  : changbodeng
# @FileName: spark_llm_query.py
import json
import time

import requests

NUM_PARTITIONS_SIZE = 200

url = 'http://query_api_service/openapi/v1/chat/query_api'
headers = {'Authorization': 'Bearer AUTH_API_ID', 'Content-Type': 'application/json'}


def build_query_prompt(input_item):
    prompt_data = input_item
    return prompt_data


def rdd_batch_query(input_data_item):
    item_prompt = build_query_prompt(input_data_item)
    data = {
        "model": "deepseek-v3",
        "search_info": True,
        "messages": [
            {"role": "user", "content": item_prompt}
        ]
    }
    response = requests.post(url=url, headers=headers, data=json.dumps(data))
    if response != 200:
        return f'query failed. input = {input_data_item}'
    result = response.json()
    return result['choices'][0]['message']['content']


def post_process(resp_text):
    return str(resp_text).replace("\n", "").replace(" ", "")


def cal_partition(partition_input_data):
    query_list = []
    for row in partition_input_data:
        query_list.append(row["input_dimension"])

    res_list = []
    for query_item in query_list:
        res, times = None, 1
        while times <= 3:
            try:
                res = rdd_batch_query(query_item)
                break
            except:
                print("request error: %d" % times)
                time.sleep(10)
                times += 1

        res = post_process(res)
        res_list.append([query_item, res])

    return res_list


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("spark_llm_api_queries").enableHiveSupport().getOrCreate()
    input_data = spark.sql("SELECT input_dimension FROM source_data")
    input_data.rdd.repartition(NUM_PARTITIONS_SIZE).mapPartitions()

    output_data = input_data.rdd.repartition(200).mapPartitions(cal_partition).toDF(["source", "response"])
    output_data.createOrReplaceTempView("output_data")

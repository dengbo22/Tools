# -*- coding: utf-8 -*-
# @Time    : 2025/6/13 11:48
# @Author  : changbodeng
# @FileName: feature_quantiles.py
import argparse

import pandas as pd
from pyspark.sql import functions as F

PROBABILITIES = 1 / 10000
N_BINS = 10

'''
CREATE TABLE db_name.output_feature_quantile_table(
    feature_index string comment '特征序列号',
    quantiles array<float> comment '分位点取值'
)
PARTITIONED BY ( 
  ds bigint,
  source_name string
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';
'''


def config_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds', type=str, required=True)
    parser.add_argument('--source_name', type=str, required=True)
    args = parser.parse_args()
    return args


def load_data(spark_entity, input_args):
    """
    :param spark_entity:
    :param input_args: 通过控制台输入的参数
    :return: 输出为以下划线作为分隔的特征表，大体格式为：feat1_feat2_feat3_..._featN，且名称为features
    """
    month_ds, source_name = input_args.ds, input_args.source_name
    data = spark_entity.sql(f"""
    SELECT user_id, split(features, "_") as features, ds
    FROM nj1_sdmpftr.t_app_feature_user_generate_underline_all_feature_mi
    WHERE ds = {month_ds} AND source = '{source_name}' 
    """).persist()
    print("num partitions = %s" % data.rdd.getNumPartitions())

    return data


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    args = config_arguments()

    month_ds = args.ds
    # spark.kryoserializer.buffer.max不得大于2g
    spark = SparkSession.builder.appName("app_name") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "1538m") \
        .config("spark.driver.maxResultSize", "10g") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    source = args.source_name
    data = load_data(spark, args)

    df_feat_dim = data.selectExpr('size(features) as sz').groupBy('sz').count().collect()
    assert len(df_feat_dim) == 1

    feat_dim = df_feat_dim[0]['sz']
    print(f"input feature name = {source}, feature dimension cnt = {feat_dim}")

    BATCH_SIZE = 16

    # Batch Computing PSI
    feature_list, quantiles = list(), list()
    for start_idx in range(0, feat_dim, BATCH_SIZE):
        # 分批次处理
        batch_index = list(range(start_idx, min(start_idx + BATCH_SIZE, feat_dim)))
        select_col = [F.col("features").getItem(i).cast("int").alias(f'num{i}') for i in batch_index]
        batch_feat = [f'num{i}' for i in batch_index]
        # 生成该批次的数据
        batch_data = data.select(*select_col, 'ds').persist()
        batch_quantiles = batch_data.approxQuantile(batch_feat, [i / N_BINS for i in range(1, N_BINS)], PROBABILITIES)

        feature_list.extend(batch_feat)
        quantiles.extend(batch_quantiles)

    quantiles_pd = pd.DataFrame({'feature': feature_list, 'bins': quantiles})
    quantiles_sdf = spark.createDataFrame(quantiles_pd)

    quantile_output = quantiles_sdf.selectExpr(
        'feature as feature_index', 'bins as quantiles', f'{month_ds} as ds', f'"{source}" as source_name'
    ).persist()
    quantile_output.show(20, False)
    quantile_output.write.insertInto(
        "db_name.output_feature_quantile_table", overwrite=True
    )

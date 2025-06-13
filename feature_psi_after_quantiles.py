# -*- coding: utf-8 -*-
# @Time    : 2025/6/13 11:48
# @Author  : changbodeng
# @FileName: feature_psi_after_quantiles.py
import argparse
from datetime import datetime
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

START_PSI_DS, COMPUTE_DS_LENGTH = 202305, 24  # 从202305月开始计算，总计计算24个月

'''
CREATE TABLE `db_name.output_feature_monthly_psi_mi`(
  `feature_index` string COMMENT 'PSI特征编号', 
  `psi_between` string COMMENT 'PSI计算的日期区间',
  `psi_score` double COMMENT 'PSI取值')
PARTITIONED BY ( 
  `ds` bigint, 
  `source` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'colelction.delim'=',', 
  'field.delim'='|', 
  'line.delim'='\n', 
  'mapkey.delim'=':', 
  'serialization.format'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
'''


# 使用时务必设置以下参数：
# spark.sql.shuffle.partitions=500;
# spark.default.parallelism=500;
# spark.hadoop.hive.exec.max.dynamic.partitions=500;

def past_n_months(venus_month_ds, month_cnt=0, output_format="%Y%m"):
    month_ds = str(venus_month_ds)[:6]

    date_ds = datetime.strptime(month_ds, "%Y%m")
    total_month = date_ds.year * 12 + date_ds.month + month_cnt
    new_year, new_month = total_month // 12, total_month % 12
    if new_month == 0:
        new_year, new_month = new_year - 1, 12

    new_ds = datetime(new_year, new_month, 1, 0, 0)
    return new_ds.strftime(output_format)


@F.udf(returnType=StringType())
def bin_udf(value, bins):
    if value is None:
        return "Missing"
    if bins is None:
        return "Missing"
    for i, b in enumerate(bins):
        if value < b:
            return f"bin_{i}"
    return f"bin_{len(bins)}"


def calculate_psi_multi_date(
        df_scores, date_col, actual_date, excepted_dates, feature_list):
    """
    特征或模型分psi计算

    :param df_scores:spark dataframe, 特征或模型分
    :param date_col:string, 日期列名
    :param actual_date:实际分布对应日期切片
    :param excepted_dates:期望分布对应日期切片list
    :param feature_list:list, 特征列表
    :param nbins:int，分箱数量
    :param probabilities: double，分位点误差，越小约精准
    :return:spark dataframe, psi结果
    """
    all_use_date = excepted_dates + [actual_date]
    df = df_scores.where(F.col(date_col).isin(all_use_date))

    # 分位点
    # 1. 读取分箱边界.
    quantiles_sdf = spark.sql(f"""
    SELECT feature_index as feature, quantiles as bins
    FROM db_name.output_feature_quantile_table
    WHERE ds = {actual_date} AND source_name = "{source_name}" 
    """)
    print(f"feature quantile dim size = {quantiles_sdf.count()}")
    quantiles_sdf.show(20, False)

    # 2. unpivot
    t_expr = []
    for f in feature_list:
        t_expr.append(f"'{f}'")
        t_expr.append(f)
    unpivotExpr = f"stack({len(feature_list)}," + ','.join(t_expr) + ') as (feature,value)'
    df_unpivot = df.select(date_col, F.expr(unpivotExpr))

    # 3. join分箱边界
    df_unpivot = df_unpivot.join(quantiles_sdf, on='feature', how='left')
    df_bin = df_unpivot.withColumn('bin', bin_udf('value', 'bins'))

    # PSI计算
    t_df1 = df_bin.groupBy('feature', date_col, 'bin').agg(F.count('bin').alias('cnt')) \
        .withColumn("all_cnt", F.sum("cnt").over(Window.partitionBy("feature", date_col))) \
        .withColumn('ratio', F.expr('cnt/all_cnt')).persist()
    t_df1.show(20, False)

    t_df2 = t_df1.groupBy('feature', 'bin').pivot(date_col, all_use_date).agg(F.first('ratio', ignorenulls=True))

    psi_colnames = []
    for excepted_date in excepted_dates:
        colname = f'psi_{actual_date}_{excepted_date}'
        psi_colnames.append(colname)
        t_df2 = t_df2.withColumn(colname
                                 , (F.col(str(actual_date)) - F.col(str(excepted_date))) * F.log(
                F.col(str(actual_date)) / F.col(str(excepted_date))))
    agg_exprs = []
    for colname in psi_colnames:
        agg_exprs.append(F.sum(colname).alias(colname))
    df_psi = t_df2.groupBy('feature').agg(*agg_exprs)
    return df_psi


FEATURE_COMPUTATION_BATCH_SIZE = 16


def config_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ds', type=str, required=True)
    parser.add_argument('--source_name', type=str, required=True)
    parser.add_argument('--ds_start', type=str, default='202304')
    parser.add_argument('--ds_len', type=int, default=24)
    args = parser.parse_args()
    return args


def load_data(spark_entity, input_args):
    """
    :param spark_entity:
    :param input_args: 通过控制台输入的参数
    :return: 输出为以下划线作为分隔的特征表，大体格式为：feat1_feat2_feat3_..._featN，且名称为features
    """
    start_ds, ds_length, month_ds = args.ds_start, args.ds_len, args.ds

    ds_total_list = [past_n_months(start_ds, i) for i in range(ds_length + 1)]
    ds_compute_list = [ds for ds in ds_total_list if ds > month_ds]
    print(f"month_ds = {month_ds}, compute psi ds = {ds_compute_list}")

    source_name = args.source_name
    loaded_data = spark_entity.sql("""
    SELECT phone_enc, split(features, "_") as features, ds
    FROM nj1_sdmpftr.t_app_feature_user_generate_underline_all_feature_mi
    WHERE ds in ({ds}) AND source = '{source}' 
    """.format(ds=','.join([month_ds, *ds_compute_list]), source=source_name))
    print("num partitions = %s" % loaded_data.rdd.getNumPartitions())

    return loaded_data


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    # Default Value

    args = config_arguments()
    month_ds, source_name = args.ds, args.source_name
    start_ds, ds_length = args.ds_start, args.ds_len

    spark = SparkSession.builder.appName("psi_after_quantiles") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "1536m") \
        .config("spark.driver.maxResultSize", "10g") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.task.maxFailures", "10") \
        .enableHiveSupport() \
        .getOrCreate()

    # 只需要读取需要计算PSI的分区.
    data = load_data(spark, args)

    df_feat_dim = data.limit(1000).selectExpr('size(features) as sz').groupBy('sz').count().collect()
    assert len(df_feat_dim) == 1
    dim_size = df_feat_dim[0]['sz']

    psi_all = []
    for start_idx in range(0, dim_size, FEATURE_COMPUTATION_BATCH_SIZE):
        # 分批次处理
        batch_index = list(range(start_idx, min(start_idx + FEATURE_COMPUTATION_BATCH_SIZE, dim_size)))
        select_col = [F.col("features").getItem(i).cast("int").alias(f"num{i}") for i in batch_index]
        # 生成该批次的数据
        batch_data = data.select(*select_col, 'ds').persist()

        feature_list = [f"num{i}" for i in batch_index]

        df_psi = calculate_psi_multi_date(batch_data, 'ds', month_ds, ds_compute_list, feature_list)
        df_psi.orderBy('feature').show(50, False)
        feature_cols = [c for c in df_psi.columns if c != "feature"]
        expr = "stack({0}, {1}) as (key, value)".format(
            len(feature_cols),
            ", ".join(["'{0}', {0}".format(c) for c in feature_cols])
        )
        df_unpivot = df_psi.selectExpr("feature", expr).persist()
        psi_all.append(df_unpivot)

    psi_union = reduce(lambda df1, df2: df1.unionAll(df2), psi_all).persist()
    print(psi_union.count())
    psi_union.show(500, False)

    psi_union.selectExpr(
        'feature as feature_index', 'key', 'value', f'{month_ds} as ds', f'"{source_name}" as source'
    ).coalesce(1).write.insertInto(
        "db_name.output_feature_monthly_psi_mi", overwrite=True
    )
    spark.stop()

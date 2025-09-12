#!/usr/bin/env python3

import argparse
import logging

import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count as _count, round as _round, col

def setup_logging():
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
        level=logging.INFO
    )
    return logging.getLogger('feature_engineering')

def parse_args():
    parser = argparse.ArgumentParser(
        description='Pipeline de Feature Engineering'
    )
    parser.add_argument(
        '--input-csv',
        required=True,
    )
    parser.add_argument(
        '--redis-host',
        default='redis',
    )
    parser.add_argument(
        '--redis-port',
        default=6379,
        type=int,
    )
    return parser.parse_args()

def compute_features(df):
    agg_df = (
        df.groupBy('cidade')
          .agg(
              _sum('capital_social').alias('capital_social_total'),
              _count('*').alias('quantidade_empresas')
          )
          .withColumn(
              'capital_social_medio',
              _round(
                  col('capital_social_total') / col('quantidade_empresas'),
                  2
              )
          )
    )
    return agg_df

def write_features_to_redis(df, host, port, logger):
    client = redis.Redis(host=host, port=port)
    for row in df.collect():
        key = row['cidade']
        mapping = {
            'capital_social_total': str(row['capital_social_total']),
            'quantidade_empresas': str(row['quantidade_empresas']),
            'capital_social_medio': str(row['capital_social_medio'])
        }
        client.hset(key, mapping=mapping)
        logger.info(f'Gravado no Redis -> {key}: {mapping}')

def main():
    args = parse_args()
    logger = setup_logging()
    logger.info('Iniciando processamento de features...')

    spark = (
        SparkSession.builder
            .appName('MarketIntelligenceFeatures')
            .getOrCreate()
    )

    raw_df = spark.read.csv(
        args.input_csv,
        header=True,
        inferSchema=True
    )
    df = raw_df.withColumn(
        'capital_social',
        col('capital_social').cast('double')
    )

    features_df = compute_features(df)
    write_features_to_redis(
        features_df,
        host=args.redis_host,
        port=args.redis_port,
        logger=logger
    )

    logger.info('Processamento de features concluído.')
    spark.stop()

if __name__ == '__main__':
    main()

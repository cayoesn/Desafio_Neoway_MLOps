import logging
import redis
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    sum as _sum,
    count as _count,
    round as _round,
    col
)
from typing import Optional


def setup_logging(name: str = 'feature_engineering') -> logging.Logger:
    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
        level=logging.INFO
    )
    return logging.getLogger(name)


def read_csv_to_df(
    spark: SparkSession,
    path: str,
    logger: logging.Logger
) -> DataFrame:
    logger.info(f'Reading CSV file: {path}')
    df = spark.read.csv(path, header=True, inferSchema=True)
    logger.info(f'File {path} loaded. Rows: {df.count()}')
    return df


def cast_columns(df: DataFrame, logger: logging.Logger) -> DataFrame:
    logger.info('Casting column "capital_social" to double...')
    return df.withColumn(
        'capital_social',
        col('capital_social').cast('double')
    )


def compute_features(df: DataFrame, logger: logging.Logger) -> DataFrame:
    logger.info('Computing features...')
    features = (
        df.groupBy('cidade')
          .agg(
              _sum('capital_social').alias('capital_social_total'),
              _count('*').alias('quantidade_empresas')
        )
        .withColumn(
              'capital_social_medio',
              _round(col('capital_social_total') /
                     col('quantidade_empresas'), 2)
        )
    )
    logger.info(f'Features computed. Cities: {features.count()}')
    return features


def write_features_to_redis(
    df: DataFrame, host: str,
    port: int,
    logger: logging.Logger
) -> None:
    logger.info(f"Connecting to Redis at {host}:{port}...")
    client = redis.Redis(host=host, port=port)
    logger.info("Writing features to Redis...")
    rows = df.collect()
    for row in rows:
        key = row['cidade']
        mapping = {
            'capital_social_total': str(row['capital_social_total']),
            'quantidade_empresas': str(row['quantidade_empresas']),
            'capital_social_medio': str(row['capital_social_medio'])
        }
        client.hset(key, mapping=mapping)
        logger.info(f'Record written to Redis -> {key}: {mapping}')
    logger.info("All features written to Redis.")


def process(
    input_csv: str,
    redis_host: str = 'redis',
    redis_port: int = 6379,
    spark: Optional[SparkSession] = None,
    logger: Optional[logging.Logger] = None
) -> None:
    logger = logger or setup_logging()
    logger.info('Starting feature engineering pipeline...')
    logger.info(f'Input file: {input_csv}')
    logger.info(f'Redis: {redis_host}:{redis_port}')

    spark = spark or SparkSession.builder.appName(
        'MarketIntelligenceFeatures').getOrCreate()
    try:
        df = read_csv_to_df(spark, input_csv, logger)
        df = cast_columns(df, logger)
        features_df = compute_features(df, logger)
        write_features_to_redis(features_df, redis_host, redis_port, logger)
        logger.info('Pipeline completed successfully.')
    finally:
        spark.stop()
        logger.info('SparkSession stopped.')


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='Feature Engineering Pipeline')
    parser.add_argument('--input-csv', required=True)
    parser.add_argument('--redis-host', default='redis')
    parser.add_argument('--redis-port', default=6379, type=int)
    args = parser.parse_args()
    process(args.input_csv, args.redis_host, args.redis_port)


if __name__ == '__main__':
    main()

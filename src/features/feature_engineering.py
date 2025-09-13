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

def compute_features(df):
    return (
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

def write_features_to_redis(df, host, port, logger):
    logger.info(f"Connecting to Redis at {host}:{port}...")
    client = redis.Redis(host=host, port=port)
    logger.info("Writing features to Redis...")
    for row in df.collect():
        key = row['cidade']
        mapping = {
            'capital_social_total': str(row['capital_social_total']),
            'quantidade_empresas':   str(row['quantidade_empresas']),
            'capital_social_medio':  str(row['capital_social_medio'])
        }
        client.hset(key, mapping=mapping)
        logger.info(f'Record written to Redis -> {key}: {mapping}')
    logger.info("All features written to Redis.")

def process(input_csv: str, redis_host: str = 'redis', redis_port: int = 6379):
    logger = setup_logging()
    logger.info('Starting feature engineering process...')
    logger.info(f'Received input file for processing: {input_csv}')
    logger.info(f'Redis target: {redis_host}:{redis_port}')

    spark = SparkSession.builder.appName('MarketIntelligenceFeatures').getOrCreate()
    logger.info('Spark session started.')
    logger.info('Reading CSV file into DataFrame...')
    raw_df = spark.read.csv(input_csv, header=True, inferSchema=True)
    logger.info(f'CSV file {input_csv} loaded. Number of rows: {raw_df.count()}')
    logger.info('Casting column "capital_social" to double...')
    df = raw_df.withColumn('capital_social', col('capital_social').cast('double'))

    logger.info('Computing features...')
    features_df = compute_features(df)
    logger.info(f'Features computed. Number of cities: {features_df.count()}')
    write_features_to_redis(features_df, host=redis_host, port=redis_port, logger=logger)

    logger.info('Feature engineering process completed.')
    spark.stop()
    logger.info('Spark session stopped.')

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Feature Engineering Pipeline')
    parser.add_argument('--input-csv', required=True, help='Path to the input CSV file')
    parser.add_argument('--redis-host', default='redis', help='Redis server host')
    parser.add_argument('--redis-port', default=6379, type=int, help='Redis server port')
    args = parser.parse_args()
    process(args.input_csv, args.redis_host, args.redis_port)

if __name__ == '__main__':
    main()

import pytest
from unittest import mock
from pyspark.sql import SparkSession
from features.feature_engineering import (
    compute_features,
    write_features_to_redis,
    setup_logging,
    parse_args,
)

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
            .master("local[2]")
            .appName("pytest-pyspark-local")
            .getOrCreate()
    )
    yield spark
    spark.stop()

def test_compute_features_basic(spark):
    data = [
        {"cidade": "São Paulo",    "capital_social": 50000.0},
        {"cidade": "São Paulo",    "capital_social": 120000.0},
        {"cidade": "Rio de Janeiro","capital_social": 75000.0},
        {"cidade": "Belo Horizonte","capital_social": 30000.0},
        {"cidade": "São Paulo",    "capital_social": 25000.0},
        {"cidade": "Rio de Janeiro","capital_social": 150000.0},
    ]
    df = spark.createDataFrame(data)
    result_df = compute_features(df)
    rows = result_df.collect()
    output = {row["cidade"]: row.asDict() for row in rows}

    sp = output["São Paulo"]
    assert sp["capital_social_total"]   == 195000.0
    assert sp["quantidade_empresas"]    == 3
    assert sp["capital_social_medio"]   == 65000.0

    rj = output["Rio de Janeiro"]
    assert rj["capital_social_total"]   == 225000.0
    assert rj["quantidade_empresas"]    == 2
    assert rj["capital_social_medio"]   == 112500.0

    bh = output["Belo Horizonte"]
    assert bh["capital_social_total"]   == 30000.0
    assert bh["quantidade_empresas"]    == 1
    assert bh["capital_social_medio"]   == 30000.0

def test_compute_features_empty(spark):
    df = spark.createDataFrame([], "cidade string, capital_social double")
    result_df = compute_features(df)
    assert result_df.count() == 0

def test_write_features_to_redis_calls_hset(spark):
    data = [
        {"cidade": "TestCity", "capital_social": 100.0},
        {"cidade": "TestCity", "capital_social": 200.0},
    ]
    df = compute_features(spark.createDataFrame(data))
    mock_redis = mock.Mock()
    mock_logger = mock.Mock()
    with mock.patch("features.feature_engineering.redis.Redis", return_value=mock_redis):
        write_features_to_redis(df, host="localhost", port=6379, logger=mock_logger)
    assert mock_redis.hset.called
    assert mock_logger.info.called

def test_setup_logging_returns_logger():
    logger = setup_logging()
    assert hasattr(logger, "info")
    assert hasattr(logger, "warning")

def test_parse_args(monkeypatch):
    test_args = [
        "prog",
        "--input-csv", "input.csv",
        "--redis-host", "localhost",
        "--redis-port", "1234"
    ]
    monkeypatch.setattr("sys.argv", test_args)
    args = parse_args()
    assert args.input_csv == "input.csv"
    assert args.redis_host == "localhost"
    assert args.redis_port == 1234
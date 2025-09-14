import pytest
import logging
from pyspark.sql import SparkSession
from features import feature_engineering
from unittest import mock


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master(
        "local[1]").appName("pytest").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def logger():
    return feature_engineering.setup_logging("test_logger")


@pytest.fixture
def sample_df(spark):
    data = [
        {"cidade": "A", "capital_social": "100.0"},
        {"cidade": "A", "capital_social": "200.0"},
        {"cidade": "B", "capital_social": "300.0"}
    ]
    return spark.createDataFrame(data)


def test_setup_logging():
    logger = feature_engineering.setup_logging("test")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test"


def test_read_csv_to_df(tmp_path, spark, logger):
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("cidade,capital_social\nA,100.0\nB,200.0\n")
    df = feature_engineering.read_csv_to_df(spark, str(csv_path), logger)
    assert df.count() == 2
    assert set(df.columns) == {"cidade", "capital_social"}


def test_cast_columns(sample_df, logger):
    df_casted = feature_engineering.cast_columns(sample_df, logger)
    assert dict(df_casted.dtypes)["capital_social"] == "double"


def test_compute_features(sample_df, logger):
    df_casted = feature_engineering.cast_columns(sample_df, logger)
    features = feature_engineering.compute_features(df_casted, logger)
    result = {row["cidade"]: row.asDict() for row in features.collect()}
    assert result["A"]["capital_social_total"] == 300.0
    assert result["A"]["quantidade_empresas"] == 2
    assert result["A"]["capital_social_medio"] == 150.0
    assert result["B"]["capital_social_total"] == 300.0
    assert result["B"]["quantidade_empresas"] == 1
    assert result["B"]["capital_social_medio"] == 300.0


def test_write_features_to_redis(sample_df, logger):
    df_casted = feature_engineering.cast_columns(sample_df, logger)
    features = feature_engineering.compute_features(df_casted, logger)
    with mock.patch(
        "features.feature_engineering.redis.Redis"
    ) as mock_redis:
        client = mock_redis.return_value
        feature_engineering.write_features_to_redis(
            features, "localhost", 6379, logger)
        assert client.hset.call_count == 2
        calls = [mock.call("A", mapping=mock.ANY),
                 mock.call("B", mapping=mock.ANY)]
        client.hset.assert_has_calls(calls, any_order=True)


def test_process(tmp_path, spark):
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("cidade,capital_social\nA,100.0\nA,200.0\nB,300.0\n")
    with mock.patch(
        "features.feature_engineering.write_features_to_redis"
    ) as mock_write:
        with mock.patch(
            "features.feature_engineering.setup_logging"
        ) as mock_logger:
            feature_engineering.process(
                str(csv_path),
                redis_host="localhost",
                redis_port=6379,
                spark=spark,
                logger=mock_logger
            )
            assert mock_write.called


def test_main(monkeypatch):
    args = ["prog", "--input-csv", "file.csv",
            "--redis-host", "localhost", "--redis-port", "6380"]
    with mock.patch(
        "features.feature_engineering.process"
    ) as mock_process:
        monkeypatch.setattr("sys.argv", args)
        feature_engineering.main()
        mock_process.assert_called_once_with("file.csv", "localhost", 6380)

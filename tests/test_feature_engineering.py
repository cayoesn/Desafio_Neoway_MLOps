import logging
import pytest
import sys
import features.feature_engineering as feature_engineering

@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-spark")
        .getOrCreate()
    )
    yield spark
    spark.stop()

def test_setup_logging_returns_logger():
    logger = feature_engineering.setup_logging()
    assert isinstance(logger, logging.Logger)
    assert logger.name == "feature_engineering"

def test_compute_features_correct_aggregation(spark):
    data = [
        ("CidadeA", 10.0),
        ("CidadeA", 20.0),
        ("CidadeB", 30.0),
    ]
    df = spark.createDataFrame(data, ["cidade", "capital_social"])
    result_df = feature_engineering.compute_features(df)
    rows = {row["cidade"]: row for row in result_df.collect()}
    assert "CidadeA" in rows
    assert rows["CidadeA"]["capital_social_total"] == 30.0
    assert rows["CidadeA"]["quantidade_empresas"] == 2
    assert rows["CidadeA"]["capital_social_medio"] == 15.0
    assert "CidadeB" in rows
    assert rows["CidadeB"]["capital_social_total"] == 30.0
    assert rows["CidadeB"]["quantidade_empresas"] == 1
    assert rows["CidadeB"]["capital_social_medio"] == 30.0

class FakeClient:
    def __init__(self):
        self.hset_calls = []

    def hset(self, key, mapping=None):
        self.hset_calls.append((key, mapping))

class FakeRow:
    def __init__(self, cidade, total, qtd, medio):
        self.cidade = cidade
        self.capital_social_total = total
        self.quantidade_empresas = qtd
        self.capital_social_medio = medio
    def __getitem__(self, item):
        return getattr(self, item)

class FakeDF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows

def test_write_features_to_redis(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    fake_client = FakeClient()
    monkeypatch.setattr(
        feature_engineering.redis, "Redis", lambda *args, **kwargs: fake_client
    )
    logger = feature_engineering.setup_logging()
    rows = [
        FakeRow("X", 100.0, 2, 50.0),
        FakeRow("Y", 300.0, 3, 100.0),
    ]
    fake_df = FakeDF(rows)
    feature_engineering.write_features_to_redis(
        fake_df, host="h", port=1234, logger=logger
    )
    assert len(fake_client.hset_calls) == 2
    expect_X = ("X", {
        "capital_social_total": "100.0",
        "quantidade_empresas":   "2",
        "capital_social_medio":  "50.0",
    })
    expect_Y = ("Y", {
        "capital_social_total": "300.0",
        "quantidade_empresas":   "3",
        "capital_social_medio":  "100.0",
    })
    assert expect_X in fake_client.hset_calls
    assert expect_Y in fake_client.hset_calls
    assert "Connecting to Redis at h:1234" in caplog.text
    assert "Writing features to Redis..." in caplog.text
    assert "Record written to Redis -> X" in caplog.text
    assert "All features written to Redis." in caplog.text


def test_process_reads_csv_and_writes(monkeypatch, tmp_path, caplog, spark):
    caplog.set_level(logging.INFO)
    data = [
        "cidade,capital_social",
        "C1,10.0",
        "C1,20.0",
        "C2,30.0",
    ]
    csv_file = tmp_path / "input.csv"
    csv_file.write_text("\n".join(data), encoding="utf-8")
    fake_client = FakeClient()
    monkeypatch.setattr(
        feature_engineering.redis, "Redis",
        lambda *args, **kwargs: fake_client
    )
    class Builder:
        def __init__(self, spark):
            self._spark = spark
        def appName(self, _): return self
        def getOrCreate(self): return self._spark
    monkeypatch.setattr(
        feature_engineering,
        "SparkSession",
        type("SS", (), {"builder": Builder(spark)})
    )
    feature_engineering.process(
        input_csv=str(csv_file),
        redis_host="h",
        redis_port=9999
    )
    calls = dict(fake_client.hset_calls)
    assert "C1" in calls
    assert calls["C1"]["capital_social_total"] == "30.0"
    assert calls["C1"]["quantidade_empresas"] == "2"
    assert calls["C1"]["capital_social_medio"] == "15.0"
    assert "C2" in calls
    assert calls["C2"]["capital_social_total"] == "30.0"
    assert calls["C2"]["quantidade_empresas"] == "1"
    assert calls["C2"]["capital_social_medio"] == "30.0"
    assert "Starting feature engineering process..." in caplog.text
    assert f"Received input file for processing: {csv_file}" in caplog.text
    assert "Feature engineering process completed." in caplog.text
    assert "Spark session stopped." in caplog.text


def test_main_invokes_process(monkeypatch):
    
    test_args = [
        "prog",
        "--input-csv", "arquivo.csv",
        "--redis-host", "h",
        "--redis-port", "1234",
    ]
    monkeypatch.setattr(sys, "argv", test_args)

    called = {}
    def fake_process(input_csv, redis_host, redis_port):
        called["args"] = (input_csv, redis_host, redis_port)

    monkeypatch.setattr(feature_engineering, "process", fake_process)
    feature_engineering.main()

    assert called["args"] == ("arquivo.csv", "h", 1234)

from dags import pipeline
import pytest
from unittest import mock


class DummyVariable:
    @staticmethod
    def get(var_name, default=None):
        return f"dummy_{var_name}" if default is None else default


class DummyRedis:
    def __init__(self, host, port, socket_connect_timeout):
        self.host = host
        self.port = port

    def ping(self):
        return True


@pytest.fixture(autouse=True)
def patch_airflow_variable(monkeypatch):
    monkeypatch.setattr("dags.pipeline.Variable", DummyVariable)


@pytest.fixture(autouse=True)
def patch_redis(monkeypatch):
    monkeypatch.setattr("dags.pipeline.redis.Redis", DummyRedis)


def test_get_env_variable_default():
    assert pipeline.get_env_variable("test_var", "default") == "default"


def test_get_env_variable_env(monkeypatch):
    monkeypatch.setenv("TEST_VAR", "env_value")
    assert pipeline.get_env_variable("test_var") == "env_value"


def test_redis_health_check_success():
    pipeline.redis_health_check()


def test_redis_health_check_failure(monkeypatch):
    class FailingRedis:
        def __init__(self, host, port, socket_connect_timeout):
            pass

        def ping(self):
            return False
    monkeypatch.setattr("dags.pipeline.redis.Redis", FailingRedis)
    with pytest.raises(Exception) as exc:
        pipeline.redis_health_check()
    assert "Redis ping failed" in str(exc.value)


def test_get_input_csv_file_exists(tmp_path, monkeypatch):
    test_csv = tmp_path / "test.csv"
    test_csv.write_text("col1,col2\n1,2")
    monkeypatch.setattr("dags.pipeline.get_env_variable",
                        lambda *a, **kw: str(test_csv))
    assert pipeline.get_input_csv() == str(test_csv)


def test_get_input_csv_file_not_exists(monkeypatch):
    fake_path = "not_a_real_file.csv"
    monkeypatch.setattr("dags.pipeline.get_env_variable",
                        lambda *a, **kw: fake_path)
    with pytest.raises(FileNotFoundError) as exc:
        pipeline.get_input_csv()
    assert "CSV file not found" in str(exc.value)


def test_get_input_csv_context(monkeypatch, tmp_path):
    test_csv = tmp_path / "context.csv"
    test_csv.write_text("header\nvalue")
    context = {"dag_run": mock.Mock(conf={"input_csv": str(test_csv)})}
    assert pipeline.get_input_csv(context) == str(test_csv)


def test_process_features(monkeypatch, tmp_path):
    test_csv = tmp_path / "proc.csv"
    test_csv.write_text("header\nvalue")
    monkeypatch.setattr("dags.pipeline.get_input_csv",
                        lambda *a, **kw: str(test_csv))
    called = {}

    def fake_process(input_csv, redis_host, redis_port):
        called.update(
            {
                "input_csv": input_csv,
                "redis_host": redis_host,
                "redis_port": redis_port
            }
        )
    monkeypatch.setattr("dags.pipeline.run_feature_engineering", fake_process)
    pipeline.process_features()
    assert called["input_csv"] == str(test_csv)
    assert called["redis_host"] == "dummy_redis_host"
    assert called["redis_port"] == "dummy_redis_port"

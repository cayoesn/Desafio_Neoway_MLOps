import pytest
from pyspark.sql import SparkSession
from features.feature_engineering import compute_features

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

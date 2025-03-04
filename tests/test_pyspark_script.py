import pytest
import yaml
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.my_pyspark_script import load_config, process_data, process_data_all

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    return SparkSession.builder.master("local").appName("test").getOrCreate()

@pytest.fixture
def sample_config(tmpdir):
    """Create a temporary YAML config file for testing."""
    config_data = {
        "dataset_url": "sh0416/ag_news",
        "words_to_count": ["president", "the", "Asia"],
        "output_path": f"{tmpdir}/"
    }
    config_path = os.path.join(tmpdir, "config.yaml")
    with open(config_path, "w") as file:
        yaml.dump(config_data, file)
    return config_path, config_data

def test_load_config(sample_config):
    """Test loading YAML configuration."""
    config_path, expected_config = sample_config
    assert load_config(config_path) == expected_config

@pytest.mark.parametrize("func,output_file", [
    (process_data, "word_count.parquet"),
    (process_data_all, "word_count_all.parquet"),
])
def test_process_data_functions(spark, sample_config, monkeypatch, func, output_file):
    """Test both word count processing functions."""
    _, config = sample_config

    # Sample DataFrame mimicking AG News dataset
    sample_data = pd.DataFrame({"description": ["The president of Asia spoke.", "The economy is growing."]})

    # Patch dataset loader
    monkeypatch.setattr("src.my_pyspark_script.load_dataset", lambda url, split: sample_data)

    # Run processing function
    func(config)

    # Verify output file exists
    assert os.path.exists(config["output_path"] + output_file)

    # Load and check results
    result_df = spark.read.parquet(config["output_path"] + output_file)
    assert result_df.filter(col("word") == "president").count() > 0

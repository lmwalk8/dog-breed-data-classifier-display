import decimal
import os
import tempfile

import pytest
from pyspark.sql import SparkSession

from scripts.transform import transform_dog_breed_data


@pytest.fixture(scope="module")
def spark_session():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.appName("Dog Breed Transform Tests")
        .master("local[1]")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse-test")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_csv_file(spark_session):
    """Create a temporary CSV file with sample dog breed data."""
    sample_data = [
        "Name,Origin,Type,Unique Feature,Friendly Rating,Life Span,Size,Grooming Needs,Exercise Requirements,Good with Children,Intelligence Rating,Shedding Level,Health Issues Risk,Average Weight (kg),Training Difficulty",
        "Beagle,England,Hound,Friendly and Curious,5,15,Medium,Moderate,2.0,Yes,4,Moderate,Low,11.0,3",
        "Golden Retriever,Scotland,Sporting,Intelligent and Friendly,5,11,Large,High,2.5,Yes,5,High,Moderate,32.0,2",
        "Border Collie,Scotland,Herding,Highly Intelligent,4,13,Medium,Moderate,3.0,Yes,5,Moderate,Low,20.0,2",
        "Chihuahua,Mexico,Toy,Small but Bold,4,15,Toy,Low,1.0,Yes,3,Low,Moderate,2.5,3",
        "Great Dane,Germany,Working,Gentle Giant,5,8,Giant,Low,2.0,Yes,4,Moderate,High,70.0,3",
    ]

    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    )
    temp_file.write("\n".join(sample_data))
    temp_file.close()

    yield temp_file.name

    # Cleanup
    if os.path.exists(temp_file.name):
        os.unlink(temp_file.name)


@pytest.fixture
def sample_csv_with_issues(spark_session):
    """Create a CSV file with data quality issues to test transformations."""
    sample_data = [
        "Name,Origin,Type,Unique Feature,Friendly Rating,Life Span,Size,Grooming Needs,Exercise Requirements,Good with Children,Intelligence Rating,Shedding Level,Health Issues Risk,Average Weight (kg),Training Difficulty",
        "Beagle,Alaska USA,Hound,Friendly,5,15,Medium,Moderate,2.0,Yes,4,Moderate,Low,11.0,3",
        "Standard Poodle,France,Non-Sporting,Intelligent,5,13,Large,High,2.0,Yes,5,Low,Moderate,25-Jul,2",
        "Border Collie,England,Herding,Smart,4,13,Medium,Moderate,3.0,Yes,5,Moderate,Low,20.0,2",
        "Miniature Schnauzer,Germany,Non-Sporting,Bold,4,13,Small,High,1.5,Yes,4,Low,Moderate,7.0,3",
    ]

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("\n".join(sample_data))
    temp_file.close()

    yield temp_file.name

    if os.path.exists(temp_file.name):
        os.unlink(temp_file.name)


@pytest.mark.transformation
def test_transform_column_renaming(spark_session, sample_csv_file):
    """Test that columns are renamed correctly."""
    df = transform_dog_breed_data(spark_session, sample_csv_file)

    assert (
        "Breed Name" in df.columns
    ), "Breed Name column should exist after transformation"
    assert (
        "Origin (Country)" in df.columns
    ), "Origin (Country) column should exist after transformation"
    assert "Name" not in df.columns, "Original Name column should not exist"
    assert "Origin" not in df.columns, "Original Origin column should not exist"


@pytest.mark.transformation
def test_transform_duplicate_removal(spark_session, sample_csv_file):
    """Test that duplicates are removed."""
    df = transform_dog_breed_data(spark_session, sample_csv_file)

    # Check that there are no duplicate breed names
    breed_names = df.select("Breed Name").distinct().count()
    total_rows = df.count()
    assert breed_names == total_rows, "No duplicate breed names should exist"


@pytest.mark.transformation
def test_transform_specific_breed_filtering(spark_session, sample_csv_file):
    """Test that specific duplicate breeds are filtered out."""
    df = transform_dog_breed_data(spark_session, sample_csv_file)

    # Check that Standard Poodle and Pyrenean Mountain Dog are not in the data
    breed_names = [row["Breed Name"] for row in df.select("Breed Name").collect()]
    assert (
        "Standard Poodle" not in breed_names
    ), "Standard Poodle should be filtered out"
    assert (
        "Pyrenean Mountain Dog" not in breed_names
    ), "Pyrenean Mountain Dog should be filtered out"


@pytest.mark.transformation
def test_transform_country_standardization(spark_session, sample_csv_with_issues):
    """Test that country names are standardized correctly."""
    df = transform_dog_breed_data(spark_session, sample_csv_with_issues)

    # Check that "Alaska USA" is converted to "USA"
    beagle_rows = df.filter(df["Breed Name"] == "Beagle").collect()
    if beagle_rows:
        assert (
            beagle_rows[0]["Origin (Country)"] == "USA"
        ), "Alaska USA should be converted to USA"

    # Check that Border Collie origin is set to Scotland
    border_collie_rows = df.filter(df["Breed Name"] == "Border Collie").collect()
    if border_collie_rows:
        assert (
            border_collie_rows[0]["Origin (Country)"] == "Scotland"
        ), "Border Collie origin should be Scotland"


@pytest.mark.transformation
def test_transform_size_classification(spark_session, sample_csv_file):
    """Test that size is correctly classified based on weight."""
    df = transform_dog_breed_data(spark_session, sample_csv_file)

    # Check size classifications
    for row in df.collect():
        weight = float(row["Average Weight (kg)"])
        size = row["Size"]

        if weight < 5:
            assert (
                size == "Toy"
            ), f"Breed with weight {weight} should be classified as Toy"
        elif weight < 10:
            assert (
                size == "Small"
            ), f"Breed with weight {weight} should be classified as Small"
        elif weight < 25:
            assert (
                size == "Medium"
            ), f"Breed with weight {weight} should be classified as Medium"
        elif weight < 40:
            assert (
                size == "Large"
            ), f"Breed with weight {weight} should be classified as Large"
        else:
            assert (
                size == "Giant"
            ), f"Breed with weight {weight} should be classified as Giant"


@pytest.mark.transformation
def test_transform_no_null_values(spark_session, sample_csv_file):
    """Test that there are no null values in the transformed data."""
    df = transform_dog_breed_data(spark_session, sample_csv_file)

    # Check for null values in all columns
    for col_name in df.columns:
        null_count = df.filter(df[col_name].isNull()).count()
        assert null_count == 0, f"Column {col_name} should not have null values"


@pytest.mark.transformation
def test_transform_weight_data_type(spark_session, sample_csv_file):
    """Test that weight is converted to Decimal type."""
    df = transform_dog_breed_data(spark_session, sample_csv_file)

    # Check that Average Weight (kg) column exists and has numeric values
    weight_col = df.select("Average Weight (kg)").collect()
    assert len(weight_col) > 0, "Weight column should have data"

    # Check that weights are numeric (not strings)
    for row in weight_col:
        weight = row["Average Weight (kg)"]
        assert weight is not None, "Weight should not be None"
        assert isinstance(
            weight, (int, float, decimal.Decimal)
        ), f"Weight should be numeric, got {type(weight)}"


@pytest.mark.transformation
def test_transform_missing_file_error(spark_session):
    """Test that transform raises FileNotFoundError for missing file."""
    with pytest.raises(FileNotFoundError):
        transform_dog_breed_data(spark_session, "/nonexistent/file.csv")


@pytest.mark.transformation
def test_transform_missing_required_columns(spark_session):
    """Test that transform raises ValueError for missing required columns."""
    # Create CSV with missing required columns
    sample_data = ["Name,Origin,Type", "Beagle,England,Hound"]

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("\n".join(sample_data))
    temp_file.close()

    try:
        with pytest.raises(ValueError, match="Required columns missing"):
            transform_dog_breed_data(spark_session, temp_file.name)
    finally:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)

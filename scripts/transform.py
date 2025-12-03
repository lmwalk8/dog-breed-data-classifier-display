from pathlib import Path
from pyspark.sql.functions import col, when
from pyspark.sql.types import DecimalType
from pyspark.sql.utils import AnalysisException

def transform_dog_breed_data(spark, csv_file_path):
    """
    Transforms the dog breed data.

    Args:
        spark (SparkSession): The Spark session.
        csv_file_path (str): The path to the CSV file.

    Returns:
        df (DataFrame): The transformed dog breed data.
    """
    # Validate CSV file exists
    if not Path(csv_file_path).exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
    
    try:
        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    except Exception as e:
        raise ValueError(f"Failed to read CSV file '{csv_file_path}': {str(e)}")
    
    # Validate required columns exist
    required_columns = ["Name", "Origin", "Average Weight (kg)", "Size"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Required columns missing from CSV: {missing_columns}. "
            f"Available columns: {df.columns}"
        )

    try:
        # Remove duplicates
        df = df.dropDuplicates()
        
        # Rename columns
        df = df.withColumnRenamed("Name", "Breed Name")
        df = df.withColumnRenamed("Origin", "Origin (Country)")
        
        # Clean up weight data
        df = df.withColumn(
            "Average Weight (kg)",
            when(col("Average Weight (kg)") == "25-Jul", "32.5").otherwise(col("Average Weight (kg)"))
        )
        df = df.withColumn("Average Weight (kg)", col("Average Weight (kg)").cast(DecimalType(precision=3, scale=1)))
    except AnalysisException as e:
        raise ValueError(f"Column operation failed during transformation: {str(e)}")
    except Exception as e:
        raise ValueError(f"Data transformation error: {str(e)}")
    
    try:
        # Categorical Data Standardization
        # Countries
        df = df.withColumn(
            "Origin (Country)",
            when(col("Origin (Country)") == "Alaska USA", "USA").otherwise(col("Origin (Country)"))
        )
        df = df.withColumn(
            "Origin (Country)",
            when((col("Breed Name") == "Border Collie") | (col("Breed Name") == "Border Terrier"), "Scotland")
            .otherwise(col("Origin (Country)"))
        )
        df = df.withColumn(
            "Origin (Country)",
            when(col("Breed Name") == "Cavalier King Charles Spaniel", "England").otherwise(col("Origin (Country)"))
        )
        # Size
        df = df.withColumn(
            "Size",
            when(col("Average Weight (kg)") < 5, "Toy")
            .when((col("Average Weight (kg)") >= 5) & (col("Average Weight (kg)") < 10), "Small")
            .when((col("Average Weight (kg)") >= 10) & (col("Average Weight (kg)") < 25), "Medium")
            .when((col("Average Weight (kg)") >= 25) & (col("Average Weight (kg)") < 40), "Large")
            .otherwise("Giant")
        )
    except AnalysisException as e:
        raise ValueError(f"Column operation failed during categorical standardization: {str(e)}")
    except Exception as e:
        raise ValueError(f"Data standardization error: {str(e)}")
    
    return df

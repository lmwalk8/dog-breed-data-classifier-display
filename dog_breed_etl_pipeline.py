import os
import getpass
import logging
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from scripts.extract import extract_dog_breed_data
from scripts.transform import transform_dog_breed_data
from scripts.load import load_data_into_database_table

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load .env file from project root
project_root = Path(__file__).parent
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    # Fallback to default behavior (current directory)
    load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("Database URL is not set in the .env file")

os.environ['HADOOP_USER_NAME'] = getpass.getuser()

def create_spark_session():
    """
    Creates a Spark session with Java compatibility settings.

    Returns:
        spark (SparkSession): The Spark session.
    """
    # Add PostgreSQL JDBC driver to Spark packages
    # This will automatically download the driver if not already available
    spark = SparkSession.builder \
        .appName("Dog Breed ETL Pipeline") \
        .config("spark.driver.extraJavaOptions", 
                "-Dio.netty.tryReflectionSetAccessible=true " +
                f"-Dhadoop.user.name={getpass.getuser()}") \
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
        .config("spark.driver.host", "localhost") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def create_database_engine(db_url):
    """
    Creates a PostgreSQL database engine based on the Database URL

    Args:
        db_url: Specifically defined URL for a database

    Returns:
        engine (database engine): The PostgreSQL database engine.
    """
    engine = create_engine(db_url)
    return engine

def etl_process():
    """
    Performs the ETL process: Extract, Transform, and Load dog breed data.

    Returns:
        None
    """
    spark = None
    engine = None
    try:
        print("\n")
        print("=" * 60)
        print("Starting Dog Breed ETL Pipeline")
        print("=" * 60)
        
        # Step 1: Create Spark session
        print("\n[1/4] Creating Spark session...")
        spark = create_spark_session()
        print("Spark session created")
        
        # Step 2: Extract data
        print("\n[2/4] Extracting dog breed data from Kaggle...")
        raw_data = extract_dog_breed_data()
        if not raw_data:
            raise ValueError("No data fetched from Kaggle CSV file")
        print(f"Data extracted from Kaggle CSV file: {raw_data}")
        
        # Step 3: Transform data
        print("\n[3/4] Transforming dog breed data...")
        df = transform_dog_breed_data(spark, raw_data)
        row_count = df.count()
        print(f"Data transformed successfully: ({row_count} total rows)")
        
        # Step 4: Load data into database
        print("\n[4/4] Loading data into PostgreSQL database...")
        engine = create_database_engine(DATABASE_URL)
        load_data_into_database_table(df, "dog_breed_data", engine)
        print("Data loaded into database successfully")
        
        print("\n" + "=" * 60)
        print("ETL Pipeline Completed Successfully!")
        print("=" * 60)
        
    except Exception as e:
        error_msg = f"Error in ETL process: {str(e)}"
        logging.error(error_msg, exc_info=True)
        print(f"\n{error_msg}")
        raise
    finally:
        # Cleanup resources
        if spark:
            print("\nCleaning up Spark session...")
            spark.stop()
        if engine:
            engine.dispose()

if __name__ == "__main__":
    etl_process()

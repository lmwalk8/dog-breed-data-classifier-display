import getpass
from urllib.parse import urlparse

def load_data_into_database_table(df, table_name, engine):
    """
    Loads PySpark DataFrame into table in PostgreSQL database using JDBC.

    Args:
        df (pyspark.sql.DataFrame): The PySpark DataFrame to be loaded into database table.
        table_name: Name of table the data will be loaded into.
        engine: The SQLAlchemy database engine (used to extract connection URL).
    """
    try:
        # Extract connection details from SQLAlchemy engine URL
        db_url = str(engine.url)
        
        # Normalize URL format for parsing
        normalized_url = db_url.replace('postgresql://', 'postgres://')
        parsed = urlparse(normalized_url)
        
        # Extract connection properties for JDBC
        hostname = parsed.hostname or 'localhost'
        port = parsed.port or 5432
        database = parsed.path.lstrip('/') if parsed.path else 'postgres'
        
        jdbc_url = f"jdbc:postgresql://{hostname}:{port}/{database}"
        
        # Get username and password from engine URL object (more reliable)
        username = engine.url.username or parsed.username
        password = engine.url.password or parsed.password
        
        # PostgreSQL requires a username - use default if not provided
        if not username:
            # Try to get system username as fallback
            username = getpass.getuser()
            # If that doesn't work, use 'postgres' as default
            if not username:
                username = "postgres"
            print(f"Warning: No username found in DATABASE_URL, using '{username}' as default")
        
        # Build properties dictionary
        properties = {
            "driver": "org.postgresql.Driver",
            "user": username
        }
        
        # Add password if provided (PostgreSQL allows no password for trust auth)
        if password is not None:
            properties["password"] = password
        else:
            print("Warning: No password provided - ensure PostgreSQL is configured for trust/peer authentication")
        
        # Write DataFrame to PostgreSQL using JDBC
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode="overwrite",
            properties=properties
        )
        
        print(f"Successfully loaded data into table '{table_name}'")
    except Exception as e:
        error_msg = f"Error loading data to PostgreSQL table: {e}"
        print(error_msg)
        raise

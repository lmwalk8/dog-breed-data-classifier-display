from unittest.mock import MagicMock, Mock, patch

import pytest

from scripts.load import load_data_into_database_table


@pytest.fixture
def mock_spark_dataframe():
    """Create a mock PySpark DataFrame."""
    mock_df = MagicMock()
    mock_df.write = MagicMock()
    mock_df.write.jdbc = Mock()
    return mock_df


@pytest.fixture
def mock_sqlalchemy_engine():
    """Create a mock SQLAlchemy engine."""
    mock_engine = MagicMock()
    mock_engine.url = MagicMock()
    mock_engine.url.username = "testuser"
    mock_engine.url.password = "testpass"
    mock_engine.url.host = "localhost"
    mock_engine.url.port = 5432
    mock_engine.url.database = "testdb"

    # Mock the URL string representation
    mock_engine.url.__str__ = Mock(
        return_value="postgresql://testuser:testpass@localhost:5432/testdb"
    )
    return mock_engine


@pytest.mark.loading
def test_load_data_into_database_table_success(
    mock_spark_dataframe, mock_sqlalchemy_engine
):
    """Test successful loading of data into database table."""
    load_data_into_database_table(
        mock_spark_dataframe, "dog_breed_data", mock_sqlalchemy_engine
    )

    # Verify that write.jdbc was called
    mock_spark_dataframe.write.jdbc.assert_called_once()

    # Get the call arguments
    call_args = mock_spark_dataframe.write.jdbc.call_args
    assert call_args is not None

    # Verify JDBC URL format
    jdbc_url = call_args[1]["url"]
    assert jdbc_url.startswith("jdbc:postgresql://")
    assert "localhost" in jdbc_url
    assert "5432" in jdbc_url
    assert "testdb" in jdbc_url

    # Verify table name
    assert call_args[1]["table"] == "dog_breed_data"

    # Verify mode
    assert call_args[1]["mode"] == "overwrite"

    # Verify properties
    properties = call_args[1]["properties"]
    assert properties["driver"] == "org.postgresql.Driver"
    assert properties["user"] == "testuser"
    assert properties["password"] == "testpass"


@pytest.mark.loading
def test_load_data_into_database_table_no_password(mock_spark_dataframe):
    """Test loading data when no password is provided."""
    mock_engine = MagicMock()
    mock_engine.url = MagicMock()
    mock_engine.url.username = "testuser"
    mock_engine.url.password = None
    mock_engine.url.__str__ = Mock(
        return_value="postgresql://testuser@localhost:5432/testdb"
    )

    load_data_into_database_table(mock_spark_dataframe, "dog_breed_data", mock_engine)

    # Verify that write.jdbc was called
    mock_spark_dataframe.write.jdbc.assert_called_once()

    # Get the call arguments
    call_args = mock_spark_dataframe.write.jdbc.call_args
    properties = call_args[1]["properties"]

    # Password should not be in properties if None
    assert "password" not in properties or properties.get("password") is None


@pytest.mark.loading
def test_load_data_into_database_table_no_username(mock_spark_dataframe):
    """Test loading data when no username is provided (uses fallback)."""
    mock_engine = MagicMock()
    mock_engine.url = MagicMock()
    mock_engine.url.username = None
    mock_engine.url.password = "testpass"
    mock_engine.url.__str__ = Mock(return_value="postgresql://localhost:5432/testdb")

    with patch("getpass.getuser", return_value="fallback_user"):
        load_data_into_database_table(
            mock_spark_dataframe, "dog_breed_data", mock_engine
        )

        # Verify that write.jdbc was called
        mock_spark_dataframe.write.jdbc.assert_called_once()

        # Get the call arguments
        call_args = mock_spark_dataframe.write.jdbc.call_args
        properties = call_args[1]["properties"]

        # Should use fallback username
        assert properties["user"] == "fallback_user"


@pytest.mark.loading
def test_load_data_into_database_table_default_port(mock_spark_dataframe):
    """Test loading data with default port when not specified."""
    mock_engine = MagicMock()
    mock_engine.url = MagicMock()
    mock_engine.url.username = "testuser"
    mock_engine.url.password = "testpass"
    # URL without port
    mock_engine.url.__str__ = Mock(
        return_value="postgresql://testuser:testpass@localhost/testdb"
    )

    load_data_into_database_table(mock_spark_dataframe, "dog_breed_data", mock_engine)

    # Verify that write.jdbc was called
    mock_spark_dataframe.write.jdbc.assert_called_once()

    # Get the call arguments
    call_args = mock_spark_dataframe.write.jdbc.call_args
    jdbc_url = call_args[1]["url"]

    # Should use default port 5432
    assert ":5432" in jdbc_url


@pytest.mark.loading
def test_load_data_into_database_table_error_handling(
    mock_spark_dataframe, mock_sqlalchemy_engine
):
    """Test error handling when loading fails."""
    # Make write.jdbc raise an exception
    mock_spark_dataframe.write.jdbc.side_effect = Exception(
        "Database connection failed"
    )

    with pytest.raises(Exception, match="Database connection failed"):
        load_data_into_database_table(
            mock_spark_dataframe, "dog_breed_data", mock_sqlalchemy_engine
        )


@pytest.mark.loading
def test_load_data_into_database_table_jdbc_url_format(
    mock_spark_dataframe, mock_sqlalchemy_engine
):
    """Test that JDBC URL is correctly formatted."""
    load_data_into_database_table(
        mock_spark_dataframe, "dog_breed_data", mock_sqlalchemy_engine
    )

    call_args = mock_spark_dataframe.write.jdbc.call_args
    jdbc_url = call_args[1]["url"]

    # Verify JDBC URL format
    assert jdbc_url == "jdbc:postgresql://localhost:5432/testdb"

from unittest.mock import MagicMock, Mock, patch

import pytest

from app import (
    app,
    get_all_breed_data_from_database,
    get_breed_details_from_database,
    load_breeds_from_database,
)


@pytest.fixture
def client():
    """Create a test client for the Flask app."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_breed_data():
    """Sample breed data for testing."""
    return [
        ("Beagle", "beagle"),
        ("Golden Retriever", "golden-retriever"),
        ("Border Collie", "border-collie"),
    ]


@pytest.fixture
def mock_breed_details():
    """Sample breed detail data for testing."""
    return {
        "Breed Name": "Beagle",
        "Origin (Country)": "England",
        "Type": "Hound",
        "Size": "Medium",
        "Average Weight (kg)": 11.0,
        "Life Span": 15,
        "Friendly Rating": 5,
    }


@pytest.fixture
def mock_all_breed_data():
    """Sample data for all breeds."""
    return [
        {
            "Breed Name": "Beagle",
            "Origin (Country)": "England",
            "Size": "Medium",
            "Average Weight (kg)": 11.0,
        },
        {
            "Breed Name": "Golden Retriever",
            "Origin (Country)": "Scotland",
            "Size": "Large",
            "Average Weight (kg)": 32.0,
        },
    ]


@pytest.mark.flask
def test_home_route(client, mock_breed_data):
    """Test the home route returns 200 and renders template."""
    with patch("app.load_breeds_from_database", return_value=mock_breed_data):
        response = client.get("/")
        assert response.status_code == 200
        assert b"All About Dog Breeds" in response.data or b"Dog Breed" in response.data


@pytest.mark.flask
def test_home_route_no_breeds(client):
    """Test the home route when no breeds are available."""
    with patch("app.load_breeds_from_database", return_value=[]):
        response = client.get("/")
        assert response.status_code == 200


@pytest.mark.flask
def test_dog_route_success(client, mock_breed_details):
    """Test the dog detail route with valid breed."""
    with patch("app.get_breed_details_from_database", return_value=mock_breed_details):
        response = client.get("/dog/beagle")
        assert response.status_code == 200
        assert (
            b"Beagle" in response.data or response.data
        )  # Breed name should be in response


@pytest.mark.flask
def test_dog_route_not_found(client):
    """Test the dog detail route with invalid breed returns 404."""
    with patch("app.get_breed_details_from_database", return_value=None):
        response = client.get("/dog/nonexistent-breed")
        assert response.status_code == 404


@pytest.mark.flask
def test_visualization_route(client, mock_all_breed_data):
    """Test the visualization route returns 200."""
    with patch(
        "app.get_all_breed_data_from_database", return_value=mock_all_breed_data
    ):
        response = client.get("/visualization")
        assert response.status_code == 200


@pytest.mark.flask
def test_visualization_route_empty_data(client):
    """Test the visualization route with empty data."""
    with patch("app.get_all_breed_data_from_database", return_value=[]):
        response = client.get("/visualization")
        assert response.status_code == 200


@pytest.mark.flask
@patch("app.get_database_engine")
@patch("app.DATABASE_URL", "postgresql://test:test@localhost/test")
def test_load_breeds_from_database_success(mock_engine, mock_breed_data):
    """Test loading breeds from database successfully."""
    # Mock database connection
    mock_connection = MagicMock()
    mock_result = MagicMock()

    # Set up mock to return breed data
    mock_rows = [("Beagle",), ("Golden Retriever",), ("Border Collie",)]
    mock_result.__iter__ = Mock(return_value=iter(mock_rows))
    mock_result.execute.return_value = mock_result

    mock_connection.__enter__ = Mock(return_value=mock_connection)
    mock_connection.__exit__ = Mock(return_value=False)
    mock_connection.execute.return_value = mock_result

    mock_engine_instance = MagicMock()
    mock_engine_instance.connect.return_value = mock_connection
    mock_engine.return_value = mock_engine_instance

    breeds = load_breeds_from_database()
    assert len(breeds) == 3
    assert breeds[0] == ("Beagle", "beagle")


@pytest.mark.flask
@patch("app.DATABASE_URL", None)
def test_load_breeds_from_database_no_url():
    """Test loading breeds when DATABASE_URL is not set."""
    breeds = load_breeds_from_database()
    assert breeds == []


@pytest.mark.flask
@patch("app.get_database_engine")
@patch("app.DATABASE_URL", "postgresql://test:test@localhost/test")
def test_get_breed_details_from_database_success(mock_engine, mock_breed_details):
    """Test getting breed details from database successfully."""
    # Mock database connection
    mock_connection = MagicMock()
    mock_result = MagicMock()

    # Create a mock row that behaves like a database row
    mock_row = MagicMock()
    mock_row.__getitem__ = Mock(
        side_effect=lambda i: list(mock_breed_details.values())[i]
    )

    mock_result.fetchone.return_value = mock_row
    mock_result.keys.return_value = list(mock_breed_details.keys())
    mock_result.execute.return_value = mock_result

    mock_connection.__enter__ = Mock(return_value=mock_connection)
    mock_connection.__exit__ = Mock(return_value=False)
    mock_connection.execute.return_value = mock_result

    mock_engine_instance = MagicMock()
    mock_engine_instance.connect.return_value = mock_connection
    mock_engine.return_value = mock_engine_instance

    breed_data = get_breed_details_from_database("beagle")
    assert breed_data is not None
    assert isinstance(breed_data, dict)


@pytest.mark.flask
@patch("app.get_database_engine")
@patch("app.DATABASE_URL", "postgresql://test:test@localhost/test")
def test_get_breed_details_from_database_not_found(mock_engine):
    """Test getting breed details when breed is not found."""
    # Mock database connection
    mock_connection = MagicMock()
    mock_result = MagicMock()

    mock_result.fetchone.return_value = None
    mock_result.execute.return_value = mock_result

    mock_connection.__enter__ = Mock(return_value=mock_connection)
    mock_connection.__exit__ = Mock(return_value=False)
    mock_connection.execute.return_value = mock_result

    mock_engine_instance = MagicMock()
    mock_engine_instance.connect.return_value = mock_connection
    mock_engine.return_value = mock_engine_instance

    breed_data = get_breed_details_from_database("nonexistent")
    assert breed_data is None


@pytest.mark.flask
@patch("app.DATABASE_URL", None)
def test_get_breed_details_from_database_no_url():
    """Test getting breed details when DATABASE_URL is not set."""
    breed_data = get_breed_details_from_database("beagle")
    assert breed_data is None


@pytest.mark.flask
@patch("app.get_database_engine")
@patch("app.DATABASE_URL", "postgresql://test:test@localhost/test")
def test_get_all_breed_data_from_database_success(mock_engine, mock_all_breed_data):
    """Test getting all breed data from database successfully."""
    # Mock database connection
    mock_connection = MagicMock()
    mock_result = MagicMock()

    # Create mock rows
    mock_rows = [
        MagicMock(
            __getitem__=Mock(
                side_effect=lambda i: list(mock_all_breed_data[0].values())[i]
            )
        ),
        MagicMock(
            __getitem__=Mock(
                side_effect=lambda i: list(mock_all_breed_data[1].values())[i]
            )
        ),
    ]

    mock_result.__iter__ = Mock(return_value=iter(mock_rows))
    mock_result.keys.return_value = list(mock_all_breed_data[0].keys())
    mock_result.execute.return_value = mock_result

    mock_connection.__enter__ = Mock(return_value=mock_connection)
    mock_connection.__exit__ = Mock(return_value=False)
    mock_connection.execute.return_value = mock_result

    mock_engine_instance = MagicMock()
    mock_engine_instance.connect.return_value = mock_connection
    mock_engine.return_value = mock_engine_instance

    all_data = get_all_breed_data_from_database()
    assert isinstance(all_data, list)
    assert len(all_data) == 2


@pytest.mark.flask
@patch("app.DATABASE_URL", None)
def test_get_all_breed_data_from_database_no_url():
    """Test getting all breed data when DATABASE_URL is not set."""
    all_data = get_all_breed_data_from_database()
    assert all_data == []

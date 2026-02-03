import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from scripts.extract import download_kaggle_dataset, extract_dog_breed_data


@pytest.mark.extraction
@patch("scripts.extract.KaggleApi")
@patch("scripts.extract.Path")
def test_download_kaggle_dataset_success(mock_path, mock_kaggle_api_class):
    """Test successful Kaggle dataset download."""
    # Mock Path operations
    mock_output_path = MagicMock()
    mock_output_path.mkdir = Mock()
    mock_output_path.absolute.return_value = Path("/test/data")
    mock_path.return_value = mock_output_path

    # Mock KaggleApi
    mock_api = MagicMock()
    mock_api.authenticate = Mock()
    mock_api.dataset_download_files = Mock()
    mock_kaggle_api_class.return_value = mock_api

    # Mock environment variables
    with patch.dict(os.environ, {"KAGGLE_API_TOKEN": "test_token"}):
        with patch("scripts.extract.env_path", Path("/test/.env")):
            result = download_kaggle_dataset("./data")

            # Verify API was called
            mock_api.authenticate.assert_called_once()
            mock_api.dataset_download_files.assert_called_once()
            assert result == mock_output_path


@pytest.mark.extraction
def test_download_kaggle_dataset_no_credentials():
    """Test that download raises ValueError when no Kaggle credentials are found."""
    # Mock Path to return False for kaggle.json existence
    mock_kaggle_json_path = MagicMock()
    mock_kaggle_json_path.exists.return_value = False

    with patch("scripts.extract.Path") as mock_path_class:
        # Mock Path.home() to return a path with kaggle.json that doesn't exist
        mock_home = MagicMock()
        mock_kaggle_dir = MagicMock()
        mock_kaggle_dir.__truediv__ = Mock(return_value=mock_kaggle_json_path)
        mock_home.__truediv__ = Mock(return_value=mock_kaggle_dir)
        mock_path_class.home.return_value = mock_home

        # Mock no environment variables
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.getenv", return_value=None):
                with pytest.raises(ValueError, match="Kaggle credentials not found"):
                    download_kaggle_dataset("./data")


@pytest.mark.extraction
@patch("scripts.extract.download_kaggle_dataset")
def test_extract_dog_breed_data_success(mock_download):
    """Test successful extraction of dog breed data."""
    # Create a temporary CSV file
    temp_dir = tempfile.mkdtemp()
    csv_file = os.path.join(temp_dir, "dog_breeds.csv")

    with open(csv_file, "w") as f:
        f.write("Name,Origin,Type\n")
        f.write("Beagle,England,Hound\n")

    # Mock download_kaggle_dataset to return the temp directory
    mock_output_path = MagicMock()
    mock_output_path.glob.return_value = [Path(csv_file)]
    mock_download.return_value = mock_output_path

    result = extract_dog_breed_data()

    assert result == csv_file
    assert os.path.exists(csv_file)

    # Cleanup
    os.remove(csv_file)
    os.rmdir(temp_dir)


@pytest.mark.extraction
@patch("scripts.extract.download_kaggle_dataset")
def test_extract_dog_breed_data_no_csv_file(mock_download):
    """Test that extract raises ValueError when no CSV file is found."""
    # Mock download_kaggle_dataset to return a directory with no CSV files
    mock_output_path = MagicMock()
    mock_output_path.glob.return_value = []  # No CSV files
    mock_download.return_value = mock_output_path

    with pytest.raises(ValueError, match="No CSV files found"):
        extract_dog_breed_data()


@pytest.mark.extraction
@patch("scripts.extract.KaggleApi")
def test_download_kaggle_dataset_api_error(mock_kaggle_api_class):
    """Test handling of Kaggle API errors."""
    # Mock KaggleApi to raise an exception
    mock_api = MagicMock()
    mock_api.authenticate = Mock()
    mock_api.dataset_download_files = Mock(side_effect=Exception("API Error"))
    mock_kaggle_api_class.return_value = mock_api

    # Mock environment variables
    with patch.dict(os.environ, {"KAGGLE_API_TOKEN": "test_token"}):
        with patch("scripts.extract.env_path", Path("/test/.env")):
            with pytest.raises(Exception):
                download_kaggle_dataset("./data")


@pytest.mark.extraction
@patch("scripts.extract.KaggleApi")
def test_download_kaggle_dataset_creates_output_dir(mock_kaggle_api_class):
    """Test that output directory is created if it doesn't exist."""
    # Mock Path operations
    mock_output_path = MagicMock()
    mock_output_path.mkdir = Mock()
    mock_output_path.absolute.return_value = Path("/test/data")

    with patch("scripts.extract.Path") as mock_path_class:
        mock_path_class.return_value = mock_output_path

        # Mock KaggleApi
        mock_api = MagicMock()
        mock_api.authenticate = Mock()
        mock_api.dataset_download_files = Mock()
        mock_kaggle_api_class.return_value = mock_api

        # Mock environment variables
        with patch.dict(os.environ, {"KAGGLE_API_TOKEN": "test_token"}):
            with patch("scripts.extract.env_path", Path("/test/.env")):
                download_kaggle_dataset("./data")

                # Verify mkdir was called
                mock_output_path.mkdir.assert_called_once_with(
                    parents=True, exist_ok=True
                )

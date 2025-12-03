import sys
import os
from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv

# Load .env file from project root (parent directory of scripts/)
script_dir = Path(__file__).parent
project_root = script_dir.parent
env_path = project_root / '.env'

# Try to load .env file from project root
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    # Fallback to default behavior (current directory)
    load_dotenv()
    if not os.getenv('KAGGLE_API_TOKEN') and not os.getenv('KAGGLE_USERNAME'):
        print(f"Warning: .env file not found at {env_path.absolute()}")
        print("Attempting to load from current working directory...")

def download_kaggle_dataset(output_dir='./data'):
    """
    Download the dog breed dataset from Kaggle.
    
    Args:
        output_dir: Directory to save the extracted files
    
    Returns:
        Path: Path object pointing to the directory containing downloaded files
    """
    try:
        # Check for Kaggle credentials before attempting authentication
        kaggle_token = os.getenv('KAGGLE_API_TOKEN')
        kaggle_username = os.getenv('KAGGLE_USERNAME')
        kaggle_key = os.getenv('KAGGLE_KEY')
        kaggle_json_path = Path.home() / '.kaggle' / 'kaggle.json'
        
        if not kaggle_token and not kaggle_username and not kaggle_key and not kaggle_json_path.exists():
            raise ValueError(
                "Kaggle credentials not found. Please set up authentication using one of these methods:\n"
                "1. Add KAGGLE_API_TOKEN to your .env file (recommended)\n"
                "2. Add KAGGLE_USERNAME and KAGGLE_KEY to your .env file\n"
                "3. Place kaggle.json in ~/.kaggle/kaggle.json\n"
                f"Current .env file location checked: {env_path.absolute()}"
            )
        
        # Initialize Kaggle API
        # authenticate() will automatically use KAGGLE_API_TOKEN if set (recommended for Kaggle 1.8+)
        # Otherwise falls back to ~/.kaggle/kaggle.json or KAGGLE_USERNAME/KAGGLE_KEY env vars
        api = KaggleApi()
        api.authenticate()

        dataset_name = 'prajwaldongre/top-dog-breeds-around-the-world'
        
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"Extracting dataset: {dataset_name}")
        print(f"Output directory: {output_path.absolute()}")
        
        # Extract the dataset
        api.dataset_download_files(
            dataset=dataset_name,
            path=str(output_path),
            unzip=True
        )
        
        print(f"Successfully extracted dataset to {output_path.absolute()}")

        return output_path
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        print(f"\n.env file location checked: {env_path.absolute()}")
        print(f".env file exists: {env_path.exists()}")
        
        # Check what credentials are actually loaded
        if env_path.exists():
            print("\nChecking environment variables from .env file:")
            print(f"  KAGGLE_API_TOKEN: {'Set' if os.getenv('KAGGLE_API_TOKEN') else 'Not set'}")
            print(f"  KAGGLE_USERNAME: {'Set' if os.getenv('KAGGLE_USERNAME') else 'Not set'}")
            print(f"  KAGGLE_KEY: {'Set' if os.getenv('KAGGLE_KEY') else 'Not set'}")
        
        print("\nTroubleshooting:")
        print("1. Make sure you have kaggle installed: pip install kaggle")
        print("2. Set up authentication (choose one method):")
        print("   RECOMMENDED (Kaggle 1.8+):")
        print("   - Go to https://www.kaggle.com/settings")
        print("   - Click 'Generate New Token' to create an API token")
        print(f"   - Add to .env file at {env_path.absolute()}:")
        print("     KAGGLE_API_TOKEN=your_token_here")
        print("   ALTERNATIVE:")
        print("   - Place kaggle.json in ~/.kaggle/kaggle.json")
        print(f"   - Or add to .env file: KAGGLE_USERNAME=your_username and KAGGLE_KEY=your_key")
        raise

def extract_dog_breed_data():
    """
    Extract the dog breed dataset from Kaggle and return CSV file path(s) for Spark to read.
    
    Returns:
        str: The file path of the extracted CSV file.
    """
    try:
        output_path = download_kaggle_dataset()

        # Find CSV file
        csv_files = list(output_path.glob('*.csv'))
        
        if not csv_files:
            raise ValueError("No CSV files found in the extracted dataset.")
        
        # Get the first CSV file found
        csv_file = csv_files[0]
        
        # Return file path as string for Spark to read
        return str(csv_file.absolute())
            
    except Exception as e:
        print(f"Error extracting dog breed data: {e}")
        raise

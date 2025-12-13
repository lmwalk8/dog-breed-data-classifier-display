import os
from pathlib import Path
from flask import Flask, render_template
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

app = Flask(__name__)

project_root = Path(__file__).parent
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    # Fallback to default behavior (current directory)
    load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

# Create database engine once
engine = None

def get_database_engine():
    """Get or create the database engine connection."""
    global engine
    if engine is None:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL is not set in the .env file")
        engine = create_engine(DATABASE_URL)
    return engine

def breed_name_to_slug(name):
    """Convert breed name to URL-friendly slug."""
    return name.lower().replace(' ', '-')

def slug_to_breed_name(slug):
    """Convert URL slug back to breed name."""
    return slug.replace('-', ' ').title()

def load_breeds_from_database():
    """Load all dog breeds from PostgreSQL database and return list of (name, slug) tuples."""
    breeds = []
    
    if not DATABASE_URL:
        print("Warning: DATABASE_URL not found. Cannot load breeds from database.")
        return breeds
    
    try:
        engine = get_database_engine()
        # Query the dog_breed_data table for all breed names
        with engine.connect() as connection:
            query = text("SELECT DISTINCT \"Breed Name\" FROM dog_breed_data WHERE \"Breed Name\" IS NOT NULL ORDER BY \"Breed Name\"")
            result = connection.execute(query)
            
            for row in result:
                breed_name = row[0].strip() if row[0] else None
                if breed_name:  # Skip empty names
                    slug = breed_name_to_slug(breed_name)
                    breeds.append((breed_name, slug))
                    
    except Exception as e:
        print(f"Error loading breeds from database: {e}")
        return None
    
    return breeds

def get_breed_details_from_database(breed):
    """
    Get all details for a specific dog breed from the database.
    
    Args:
        breed: URL-friendly name of the breed (e.g., 'beagle')
    
    Returns:
        dict: Dictionary containing all breed details, or None if not found
    """
    if not DATABASE_URL:
        return None
    
    try:
        engine = get_database_engine()
        
        with engine.connect() as connection:
            # Query all columns for the specific breed using case-insensitive matching
            query = text("""
                SELECT * FROM dog_breed_data 
                WHERE LOWER(REPLACE("Breed Name", ' ', '-')) = LOWER(:breed)
                LIMIT 1
            """)
            result = connection.execute(query, {"breed": breed})
            row = result.fetchone()
            
            if row:
                # Get column names from result
                try:
                    columns = result.keys()
                except AttributeError:
                    columns = result.column_names if hasattr(result, 'column_names') else [desc[0] for desc in result.cursor.description]
                
                # Create dictionary from row data
                breed_data = dict(zip(columns, row))
                return breed_data
            else:
                return None
                
    except Exception as e:
        print(f"Error loading breed details from database: {e}")
        return None

def get_all_breed_data_from_database():
    """
    Get all dog breed data from the database for visualizations.
    
    Returns:
        list: List of dictionaries containing all breed data, or empty list if error
    """
    if not DATABASE_URL:
        print("Warning: DATABASE_URL not found. Cannot load breeds from database.")
        return []
    
    try:
        engine = get_database_engine()
        with engine.connect() as connection:
            # Query all dog breed data
            query = text("SELECT * FROM dog_breed_data ORDER BY \"Breed Name\"")
            result = connection.execute(query)
            
            # Get column names
            columns = result.keys()
            
            # Convert rows to list of dictionaries
            breeds = []
            for row in result:
                breed_dict = {}
                for i, col in enumerate(columns):
                    value = row[i]
                    # Convert database types to JSON-serializable types
                    if value is None:
                        breed_dict[col] = None
                    elif hasattr(value, 'isoformat'):  # datetime objects
                        breed_dict[col] = value.isoformat()
                    elif hasattr(value, '__float__'):  # Decimal, etc.
                        try:
                            breed_dict[col] = float(value)
                        except (ValueError, TypeError):
                            breed_dict[col] = str(value)
                    else:
                        breed_dict[col] = value
                breeds.append(breed_dict)
            
            return breeds
            
    except Exception as e:
        print(f"Error loading breed data: {e}")
        return []

@app.route("/")
def home():
    breeds = load_breeds_from_database()
    return render_template('index.html', title='Home Page', breeds=breeds)

@app.route("/dog/<breed>")
def dog(breed):
    breed_data = get_breed_details_from_database(breed)
    
    if not breed_data:
        # Breed not found - return 404 or redirect
        return render_template('dog.html', breed=breed, breed_data=None, error="Breed not found"), 404
    
    return render_template('dog.html', breed=breed, breed_data=breed_data)

@app.route("/visualization")
def visualization():
    breed_data = get_all_breed_data_from_database()
    return render_template('visualization.html', title='Dog Breed Data Visualizations', breed_data=breed_data)

if __name__ == '__main__':
    app.run(debug=True)

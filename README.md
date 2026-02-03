# Dog Breed Data Display

This project uses an ETL pipeline on a public dog breed dataset. After going through the pipeline (written in Python), the processed data now loaded into a PostgreSQL table is used on a Flask webpage that allows users to view information by breed.

## Project Overview:

- Extract dog breed dataset from public Kaggle page.
- Transform raw data using PySpark. Rename columns, remove duplicate dog breeds, and replace missing or incorrect data.
- Load transformed data into PostgreSQL database table.
- Run flask application now using processed data.

## Technology Stack (Prerequisites to Run Project):

- Python 3.7+
    - Version required for PySpark compatibility
    - Libraries Used:
        - `flask`: For Flask app web framework.
        - `sqlalchemy`: For PostgreSQL interactions.
        - `dotenv`: For environment variables (DB credentials).
        - `pyspark`: For data processing.
        - `kaggle`: For Kaggle API client.
- Java JDK 8 or 11
    - Version required for PySpark
- PostgreSQL (database and user set up)
- Existing Kaggle account

## Kaggle Dataset Information

URL: [150+ Dog Breeds Around the World](https://www.kaggle.com/datasets/prajwaldongre/top-dog-breeds-around-the-world)

| Column Name | Data Type | Information |
|---|---|---|
| Name | String | The common name of the dog breed. |
| Origin | String | The country where the breed originated. |
| Type | String | The breed classification (e.g. Sporting, Terrier, Working). |
| Unique Feature | String | A distinctive physical or behavioral trait of the breed. |
| Friendly Rating | Integer | An assessment of the breed's typical temperament and friendliness towards humans. |
| Life Span | Integer | The average lifespan of the breed in years. |
| Size | String | The typical size classification of the breed (Toy, Small, Medium, Large, Giant). |
| Grooming Needs | String | The level of grooming required for the breed's coat. |
| Exercise Requirements | Decimal | The average amount of daily exercise the breed needs. |
| Good with Children | String | Whether the breed is well-suited for families with children. |
| Intelligence Rating | Integer | An assessment of the breed's trainability and problem-solving abilities.|
| Shedding Level | String | The amount the breed typically sheds. |
| Health Issues Risk | String | The likelihood of the breed developing common health problems. |
| Average Weight | Decimal | The typical weight for the breed. |
| Training Difficulty | Integer | An assessment of how challenging the breed is to train. |

Most dog breed images are from the following dataset: [Dog Breeds Image Dataset](https://www.kaggle.com/datasets/darshanthakare/dog-breeds-image-dataset/)

## Steps for Project Setup:

1. Install/create project dependencies if applicable (Python, Java, PostgreSQL, Kaggle account)

2. Clone this repository:
```
git clone https://github.com/lmwalk8/dog-breed-data-classifier-display.git
cd dog-breed-data-classifier-display
```

3. Create and activate a Python virutal environment:
```
python3 -m venv dog_breed_project_env
source dog_breed_project_env/bin/activate (Linux/macOS) OR dog_breed_project_env\Scripts\activate.bat (Windows)
```

4. Install all required dependencies:
```
pip install -r requirements.txt
```

5. Set up required environment variables:
```
Create .env variable in project directory and add this database information:
DATABASE_URL=postgresql://your_user:your_password@host:port/database_name
```
And add one of these options for Kaggle information:
- Option 1:
```
KAGGLE_API_TOKEN=your_token_here
```
- Option 2:
```
KAGGLE_USERNAME=your_username
KAGGLE_KEY=your_key
```
- Option 3:
Place kaggle.json in ~/.kaggle/kaggle.json
*More info on Kaggle setup in extract.py if needed*

6. Run ETL pipeline:
```
python dog_breed_etl_pipeline.py
```

7. Run Flask app
```
python app.py
```
OR
```
flask run
```

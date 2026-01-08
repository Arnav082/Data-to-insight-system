-->Data-to-Insight System

This project is part of my internship assignment.
The goal was to build an end-to-end data pipeline that takes raw data, processes it, and makes it searchable through an API.

I used Spark for ETL, Parquet as storage, TF-IDF for embeddings, Qdrant for vector search, and FastAPI to expose the results.

📌 What this project does

Reads raw NYC taxi data (CSV)

Cleans and transforms the data using Spark

Stores processed data in Silver and Gold layers

Converts Gold data into text and embeddings

Stores embeddings in a vector database (Qdrant)

Allows users to query insights using a FastAPI service

Example question:

“What was the average taxi fare?”

🏗️ Overall Flow
Raw CSV
  ↓
Spark ETL
  ↓
Silver Parquet
  ↓
Gold Parquet (KPIs)
  ↓
TF-IDF Embeddings
  ↓
Qdrant
  ↓
FastAPI Search API

🧰 Technologies Used

Apache Spark (PySpark) – data processing

Parquet – data lake storage

scikit-learn (TF-IDF) – text vectorization

Qdrant – vector database

FastAPI – REST API

Docker & Docker Compose – containerization

📂 Project Structure
project-root/
├── docker-compose.yml
├── Dockerfile
│
├── data/
│   ├── raw/
│   ├── silver/
│   └── gold/
│
├── spark-apps/
│   └── etl.py
│
├── vector/
│   ├── embed_and_index.py
│   └── tfidf_model.pkl
│
├── app/
│   └── main.py

🔄 Data Pipeline Explanation
1️⃣ ETL using Spark (etl.py)

Loads raw taxi CSV data

Cleans columns like fare and pickup time

Creates:

Silver layer → cleaned data

Gold layer → aggregated KPIs like average fare and total trips

2️⃣ Embedding & Indexing (embed_and_index.py)

Reads data from the Gold layer

Converts each row into a text summary

Uses TF-IDF to create vectors

Saves the trained model as tfidf_model.pkl

Indexes vectors into Qdrant

This step runs once before starting the API.

3️⃣ FastAPI Service (main.py)

Loads the saved TF-IDF model at startup

Connects to Qdrant

Exposes endpoints to search KPIs using natural language

🌐 API Endpoints
Health Check
GET /


Returns the current system status and shows whether:

model is loaded

Qdrant is connected

Search KPIs
GET /search?query=average fare


Example response:

{
  "results": [
    {
      "score": 0.82,
      "text": "On 2016-01-01, the average taxi fare was 12.45 dollars with 45678 trips."
    }
  ]
}

🐳 How to Run the Project
1️⃣ Start required services
docker compose up -d qdrant spark

2️⃣ Run Spark ETL
docker exec -it spark python3 /opt/spark-apps/etl.py

3️⃣ Run embedding and indexing (one time)
docker exec -it spark python3 /vector/embed_and_index.py


This generates the TF-IDF model file.

4️⃣ Start FastAPI
docker compose up -d fastapi

5️⃣ Test the API
curl "http://localhost:8000/search?query=average fare"

🧠 Why I chose this approach

Used TF-IDF instead of heavy transformer models to keep the system lightweight

Separated offline indexing from online querying

Used Qdrant to support semantic similarity instead of normal SQL search

Designed the pipeline similar to real production systems

🚀 Future Improvements

Add Airflow for scheduling

Add MinIO for object storage

Add LLM-based answer generation

Add caching for repeated queries

Improve monitoring and logging

📌 What I learned

Building ETL pipelines with Spark

Working with data lake layers

Using vector databases for semantic search

Containerizing multi-service systems

Debugging real-world data engineering issues
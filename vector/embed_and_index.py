import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
import joblib
import uuid
import os


print("--- Reading data...")
df = pd.read_parquet("/data/gold/taxi_kpis")


if df.empty:
    print("❌ Error: Dataset is empty.")
    exit(1)

df["summary"] = df.apply(
    lambda row: f"On {row['pickup_date']}, average fare was {row['avg_fare']:.2f}",
    axis=1
)
texts = df['summary'].tolist()


print("--- Vectorizing text...")

vectorizer = TfidfVectorizer(max_features=384, stop_words='english')
vectors = vectorizer.fit_transform(texts).toarray().tolist()


actual_dimension = len(vectors[0])
print(f"--- Actual Vector Dimension: {actual_dimension}")


joblib.dump(vectorizer, "/data/tfidf_model.pkl")
print("--- Model saved.")


client = QdrantClient(host='qdrant', port=6333)
collection_name = "taxi_kpis"


client.recreate_collection(
    collection_name=collection_name,
    vectors_config=VectorParams(
        size=actual_dimension,  
        distance=Distance.COSINE
    )
)


print(f"--- Indexing {len(vectors)} points...")
points = []
for i, vector in enumerate(vectors):
    points.append(
        PointStruct(
            id=str(uuid.uuid4()),
            vector=vector,
            payload={
                "table": "gold.taxi_kpis",
                "text": texts[i]
            }
        )
    )

client.upsert(collection_name=collection_name, points=points)
print("--- Success! Data indexed without dimension errors.")
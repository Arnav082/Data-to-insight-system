from fastapi import FastAPI, HTTPException
from qdrant_client import QdrantClient
import joblib
import os
import sys

app = FastAPI(title="Taxi KPI Search Engine")

# --- Config ---
# Try 'qdrant' first (docker), then 'localhost' (local)
QDRANT_HOST = "qdrant" 
QDRANT_PORT = 6333
MODEL_PATH = "/data/tfidf_model.pkl"
COLLECTION_NAME = "taxi_kpis"

# Global state
state = {
    "vectorizer": None,
    "client": None,
    "status": "starting",
    "error": None
}

@app.on_event("startup")
def startup_event():
    """Attempts to load resources but DOES NOT CRASH if they fail."""
    global state
    
    # 1. Try Loading Model
    if os.path.exists(MODEL_PATH):
        try:
            state["vectorizer"] = joblib.load(MODEL_PATH)
            print(f"--- Model loaded from {MODEL_PATH}")
        except Exception as e:
            print(f"--- Model exists but failed to load: {e}")
            state["error"] = f"Model load error: {e}"
    else:
        print(f"--- Warning: Model file not found at {MODEL_PATH}")
        state["error"] = f"File not found: {MODEL_PATH}. Did you run embed_and_index.py?"

    # 2. Try Connecting to Qdrant
    try:
        # Fallback logic for host
        try:
            client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=2)
            client.get_collections() # Test connection
            state["client"] = client
            print(f"--- Connected to Qdrant at {QDRANT_HOST}")
        except:
            print(f"--- Could not reach {QDRANT_HOST}, trying localhost...")
            client = QdrantClient(host="localhost", port=QDRANT_PORT)
            client.get_collections()
            state["client"] = client
            print("--- Connected to Qdrant at localhost")
            
    except Exception as e:
        print(f"--- Qdrant Connection Failed: {e}")
        if not state["error"]: 
            state["error"] = f"Qdrant Error: {e}"
    
    state["status"] = "ready" if (state["vectorizer"] and state["client"]) else "degraded"

@app.get("/")
def health_check():
    # This endpoint tells you EXACTLY what is broken
    return {
        "status": state["status"],
        "error_details": state["error"],
        "model_loaded": state["vectorizer"] is not None,
        "qdrant_connected": state["client"] is not None
    }

@app.get("/search")
def search_kpis(query: str):
    if state["status"] != "ready":
        raise HTTPException(
            status_code=503,
            detail=f"System not ready. Error: {state['error']}"
        )

    # Transform query
    vector = state["vectorizer"].transform([query]).toarray()[0].tolist()

    # Qdrant search (NEW API)
    response = state["client"].query_points(
        collection_name=COLLECTION_NAME,
        query=vector,
        limit=3
    )

    hits = response.points

    return {
        "results": [
            {
                "score": h.score,
                "text": h.payload.get("text") if h.payload else None
            }
            for h in hits
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
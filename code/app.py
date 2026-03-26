from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import os

app = FastAPI(title="Tayara Vehicles API", description="API serving processed Tayara vehicle listings.")

# Setup CORS for the React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'data', 'vehicles_gold.json')

@app.get("/api/health")
def health_check():
    return {"status": "ok"}

@app.get("/api/vehicles")
def get_vehicles():
    try:
        if not os.path.exists(DATA_FILE):
             raise HTTPException(status_code=404, detail="Data file not found")
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

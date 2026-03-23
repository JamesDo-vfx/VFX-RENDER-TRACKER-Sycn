import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os

app = FastAPI(title="Houdini Render Tracker - Firebase")

base_path = os.path.dirname(os.path.abspath(__file__))
index_path = os.path.join(base_path, "index.html")

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return f.read()
    return f"index.html not found at {index_path}"

# Mount current directory to serve style.css and other static assets
app.mount("/", StaticFiles(directory=base_path), name="root")

if __name__ == "__main__":
    print("\n--- RENDER DASHBOARD (FIREBASE MODE) IS RUNNING ---")
    print("Click here to open: http://localhost:8000")
    print("--------------------------------------------------\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)

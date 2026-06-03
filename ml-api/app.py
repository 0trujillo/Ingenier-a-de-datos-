from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {"status": "ok"}

@app.post("/predict")
def predict(data: dict):
    temp = data["temp"]

    if temp > 75:
        return {"risk": "alto"}
    return {"risk": "normal"}
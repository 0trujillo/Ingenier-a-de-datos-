from fastapi import FastAPI
import joblib

app = FastAPI()

model = joblib.load("model.pkl")


@app.get("/")
def home():
    return {
        "status": "ok"
    }


@app.post("/predict")
def predict(data: dict):

    temp = data["temp"]
    humidity = data["humidity"]
    wind = data["wind"]

    prediction = model.predict([
        [
            temp,
            humidity,
            wind
        ]
    ])

    probability = model.predict_proba([
        [
            temp,
            humidity,
            wind
        ]
    ])

    risk_score = round(
        probability[0][1] * 100,
        2
    )

    return {
        "risk_score": risk_score,
        "risk_level":
            "ALTO"
            if prediction[0] == 1
            else "NORMAL"
    }
import pandas as pd
import joblib

from sklearn.ensemble import RandomForestClassifier

data = []

for temp in range(0, 50):
    for humidity in range(5, 100, 5):
        for wind in range(0, 40, 5):

            risk = 0

            if temp > 35:
                risk += 40

            if humidity < 20:
                risk += 30

            if wind > 25:
                risk += 30

            label = 1 if risk >= 70 else 0

            data.append([
                temp,
                humidity,
                wind,
                label
            ])

df = pd.DataFrame(
    data,
    columns=[
        "temperature",
        "humidity",
        "wind_speed",
        "risk"
    ]
)

X = df[
    [
        "temperature",
        "humidity",
        "wind_speed"
    ]
]

y = df["risk"]

model = RandomForestClassifier(
    n_estimators=100,
    random_state=42
)

model.fit(X, y)

joblib.dump(
    model,
    "model.pkl"
)

print("Modelo entrenado")
scores = [0, 25, 50, 62, 74.9, 75, 90, 100]
for score in scores:
    if score >= 75:
        level = "ALTO"
    elif score >= 50:
        level = "MEDIO"
    else:
        level = "NORMAL"
    print(f"score={score:6} -> {level}")

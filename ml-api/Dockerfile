FROM python:3.11

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

RUN python ml-api/train.py

CMD ["uvicorn", "ml-api.app:app", "--host", "0.0.0.0", "--port", "8000"]

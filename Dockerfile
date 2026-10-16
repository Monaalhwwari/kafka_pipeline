FROM python:3.10-slim

WORKDIR /app

copy requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

CMD ["python", "transforming.py"]
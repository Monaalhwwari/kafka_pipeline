FROM Python:3.12

WORKDIR /app

copy requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/


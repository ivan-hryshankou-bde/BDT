FROM apache/airflow:3.1.5

COPY requirements.txt .
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt
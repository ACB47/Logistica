FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY jobs/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs/ /app/jobs
COPY ingesta/ /app/ingesta

ENV PYTHONPATH=/app

CMD ["python", "-c", "print('Setup completo')"]

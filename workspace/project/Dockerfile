FROM bitnami/spark:3.5.0

USER root

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install py4j==0.10.9.7 pandas==2.2.1 pyarrow==15.0.2 pytest==8.1.1

COPY . .

RUN pip install -r requirements.txt

ENV PYTHONPATH "$PYTHONPATH:/app/src"
ENV PYARROW_IGNORE_TIMEZONE "1"

RUN chown -R 1001:1000 /app && \
    chmod -R 755 /app

USER 1001

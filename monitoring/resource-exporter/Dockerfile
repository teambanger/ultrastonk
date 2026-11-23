FROM python:3.12-slim
WORKDIR /app
COPY exporter.py .
COPY test_exporter.py .
RUN python -m unittest -v test_exporter
ENTRYPOINT ["python", "-u", "exporter.py"]

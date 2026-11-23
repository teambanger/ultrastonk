FROM python:3.12-slim
WORKDIR /benchmarks
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY driver.py .
COPY verify.py .
COPY capture_pilot.py .
COPY drop_vs_grace.py .
ENTRYPOINT ["python", "-u", "driver.py"]

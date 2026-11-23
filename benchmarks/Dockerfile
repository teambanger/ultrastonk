FROM python:3.12-slim
WORKDIR /benchmarks
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY driver.py .
COPY verify.py .
ENTRYPOINT ["python", "-u", "driver.py"]

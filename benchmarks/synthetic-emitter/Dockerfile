FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dummy_emitter.py .
COPY tests tests
RUN python -m unittest discover -s tests -v
USER 65532:65532
ENTRYPOINT ["python", "-u", "dummy_emitter.py"]

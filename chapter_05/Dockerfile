FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your chapter code
COPY producers ./producers

# Cloud Run expects the container to listen on $PORT (defaults to 8080)
ENV PORT=8080

# Start FastAPI
CMD ["uvicorn", "producers.src.main:app", "--host", "0.0.0.0", "--port", "8080"]

FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

# Install only runtime deps
COPY etl/requirements.txt etl/requirements.txt
RUN pip install --no-cache-dir -r etl/requirements.txt

# Copy source (and data path)
COPY . .

# Default command (overridable)
CMD ["python", "-m", "etl.main"]
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    g++ \
    make \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create analysis directory and copy analysis script
RUN mkdir -p /app/analysis
COPY analysis/run_analysis.py /app/analysis/run_analysis.py

# Make script executable
RUN chmod +x /app/analysis/run_analysis.py

# Set Python path
ENV PYTHONPATH=/app

# Run the analysis script
CMD ["python", "/app/analysis/run_analysis.py"]

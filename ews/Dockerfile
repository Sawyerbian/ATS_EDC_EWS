# Use official Python base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy only the necessary project files
COPY . /app/

# Install required packages
RUN pip install --no-cache-dir -r requirements.txt

# Set environment variable (if you want to use default path)
ENV xlsx_dir_path=/app/xlsx_files

# Default command to run
CMD ["python", "/app/main.py"]

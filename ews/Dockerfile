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


# docker build -t your-image-name:tag .
# docker tag ats-edc-ews:latest sawyerbian/ats-edc-ews:latest
# docker push sawyerbian/ats-edc-ews:latest

# docker stop your-container-name
# docker rm your-container-name
# docker run -d --name ats-edc-ews --env-file ../.env -p 8000:8000 -v ./:/app/ --restart unless-stopped sawyerbian/ats-edc-ews:latest
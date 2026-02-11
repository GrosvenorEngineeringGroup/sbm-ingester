# CIM Exporter Lambda - Playwright Container Image
#
# Uses Microsoft's official Playwright Python image as base and adds
# AWS Lambda Runtime Interface Client for Lambda execution.

FROM mcr.microsoft.com/playwright/python:v1.50.0-noble

# Set working directory
WORKDIR /var/task

# Install AWS Lambda Runtime Interface Client
RUN pip install --no-cache-dir awslambdaric

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY cim_shared/ ./cim_shared/
COPY report_exporter/ ./report_exporter/

# Set Python path to include the task directory
ENV PYTHONPATH=/var/task

# Set the Lambda entry point
ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["report_exporter.app.lambda_handler"]

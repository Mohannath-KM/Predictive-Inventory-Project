FROM python:3.10-slim

WORKDIR /code

# Copy requirements file first to take advantage of Docker layer caching
COPY requirements.txt .

# Install dependencies inside the container image
RUN pip install --no-cache-dir -r requirements.txt

# Copy all remaining project files into the container
COPY . .

EXPOSE 8000

# Command to launch the FastAPI server inside the container
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
FROM python:3.11-slim

WORKDIR /app

# Install Python dependencies for the dataset api
RUN pip install --no-cache-dir \
    fastapi==0.115.6 \
    uvicorn==0.32.1 \
    pandas==2.2.3

COPY main.py ./

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8001

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]

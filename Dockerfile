FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV HOST=0.0.0.0
ENV PORT=5000
ENV FLASK_DEBUG=0

EXPOSE 5000

CMD ["python", "app.py"]

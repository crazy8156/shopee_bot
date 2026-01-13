FROM python:3.9-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

# Render will provide the PORT env var (usually 10000)
# We use sh -c to ensure the variable is expanded at runtime
CMD sh -c "streamlit run app.py --server.port=${PORT:-8501} --server.address=0.0.0.0"
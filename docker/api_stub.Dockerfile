FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Only what api_stub actually needs — not the full Airflow requirements.txt
# superset — so this image stays small and fast to build.
RUN pip install --no-cache-dir \
    fastapi==0.115.6 \
    "uvicorn[standard]==0.32.1" \
    pydantic==2.9.2

COPY api_stub/ api_stub/

CMD ["uvicorn", "api_stub.main:app", "--host", "0.0.0.0", "--port", "8000"]

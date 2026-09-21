FROM python:3.12-slim
LABEL authors="taboua-freddy"

# Dépendances système (optionnel selon ton code)
# RUN apt-get update && apt-get install -y gcc libpq-dev curl && rm -rf /var/lib/apt/lists/*

# Dossier de travail
WORKDIR /app

# Copier les fichiers
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

RUN addgroup --system app \
    && adduser --system --ingroup app app \
    && mkdir -p /app/resources/logs /app/resources/tmp /app/resources/data \
    && chown -R app:app /app

COPY --chown=app:app main.py .
COPY --chown=app:app modules/ ./modules/
COPY --chown=app:app scripts/ ./scripts/
COPY --chown=app:app config/ ./config/

# Variables d’environnement
ENV PYTHONUNBUFFERED=1

USER app

# Point d’entrée : les arguments Docker/Cloud Run sont transmis à main.py.
ENTRYPOINT ["python", "main.py"]

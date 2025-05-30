FROM python:3.11-alpine
LABEL authors="taboua-freddy"

# Dépendances système (optionnel selon ton code)
# RUN apt-get update && apt-get install -y gcc libpq-dev curl && rm -rf /var/lib/apt/lists/*

# Dossier de travail
WORKDIR /app

# Copier les fichiers
COPY requirements.txt .
COPY main.py .
COPY modules/ ./modules/
COPY credentials/ ./credentials/
COPY .env .env

# Installer les dépendances
RUN pip install --upgrade pip && pip install -r requirements.txt

# Variables d’environnement
ENV PYTHONUNBUFFERED=1

# Point d’entrée
CMD ["python", "main.py"]

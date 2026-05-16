#  GoodAir — Plateforme Big Data Qualité de l'Air

> Pipeline de collecte, transformation et analyse de données environnementales sur **20 villes françaises** en temps réel.

**Projet académique MSPR Bloc 3 — EPSI / Expert Ingénierie des Données (RNCP36921) · 2024-2025**

---

##  Contexte business

GoodAir, laboratoire R&D de TotalGreen, a besoin de données fiables toutes les heures pour permettre à ses chercheurs de surveiller la qualité de l'air, détecter les variations extrêmes et anticiper les pics de pollution sur les principales villes de France.

## Architecture
## 🛠️ Stack technique

| Couche | Outils |
|---|---|
| Orchestration | Apache Airflow |
| Traitement | PySpark |
| Stockage brut | HDFS |
| Base de données | PostgreSQL |
| Visualisation | Metabase |
| Monitoring | Prometheus · AlertManager · Grafana |
| Conteneurisation | Docker · Docker Compose |

## 📊 Données collectées

- **20 villes** : Paris, Marseille, Lyon, Toulouse, Nice, Nantes, Strasbourg, Montpellier, Bordeaux, Lille...
- **Qualité de l'air** (AQICN) : AQI global · PM2.5 · PM10 · NO2 · O3 · CO · SO2
- **Météo** (OpenWeatherMap) : Température · Humidité · Vent · Pression · Couverture nuageuse
- **Collecte** : toutes les heures · stockage incrémental · partitionnement par date/heure

## 🚀 Lancer le projet

```bash
# 1. Cloner le repo
git clone https://github.com/Karlitoprg/goodair-bigdata-platform.git
cd goodair-bigdata-platform

# 2. Configurer les variables d'environnement
cp .env.example .env
# Remplis .env avec tes tokens AQICN et OpenWeatherMap

# 3. Lancer l'infrastructure
docker-compose up -d

# 4. Accéder aux interfaces
# Airflow   → http://localhost:8080
# Metabase  → http://localhost:3000
# Grafana   → http://localhost:3001
```

## 🗄️Modélisation PostgreSQL
## 🔒 Sécurité & RGPD

- Aucun secret dans le code — variables d'environnement via `.env`
- Hébergement et traitements localisés en France / UE
- Accès aux données sécurisé par authentification (Airflow, Metabase)

## 👥 Équipe

Projet réalisé en équipe de 4 dans le cadre de la certification EPSI Expert Ingénierie des Données.


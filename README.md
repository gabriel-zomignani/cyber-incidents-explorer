# Cyber Security Events App

A student Agile process project for exploring and analyzing global cyber security events data using AI-powered analysis.

## Overview

The Cyber Security Events App is a full-stack application that combines data engineering, web development, and AI analysis to explore cyber security incidents from 2014-2025.

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Application Stack                     │
│                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  React UI    │  │  Flask API   │  │  AI Engine   │  │
│  │  (Vite)      │◄─┤  (Python)    │◄─┤  (CrewAI)    │  │
│  │  Port: 5173  │  │  Port: 5000  │  │  (Ollama)    │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│                            │                             │
│                    ┌───────▼────────┐                    │
│                    │  SQLite DB     │                    │
│                    │  (14,720 rows) │                    │
│                    └────────────────┘                    │
└─────────────────────────────────────────────────────────┘
```

### Key Features

- **Interactive Data Table** - Browse, filter, and search 14,720+ cyber security events
- **Visual Analytics** - Interactive pie charts and data visualizations
- **AI-Powered Analysis** - Ask natural language questions about cyber threats using CrewAI
- **Role-Based Access** - Admin and analyst user roles
- **ETL Pipeline** - Automated data cleaning and transformation
- **PDF Reports** - Generate downloadable reports with charts

## Quick Start with Docker Compose

### Prerequisites

- **Docker Desktop** - Download from [docker.com](https://www.docker.com/products/docker-desktop)
- **Ollama** - Required for AI features
  ```bash
  brew install ollama
  ollama serve
  ```
  On another terminal:
  This will produce a link. Follow the link and login with your credentails. This will create a Device API key automatically. 
  ```bash
  ollama signin
  ```
  Pull cloud model. 
  ```
  ollama pull gpt-oss:120b-cloud
  ```

### Running the Application

1. **Clone the repository**
   ```bash
   git clone https://github.com/gabriel-zomignani/cyber-incidents-explorer.git
   cd cyber-security-events-app
   ```

2. **Start all services**
   ```bash
   docker compose up --build
   ```
   or use docker-compose-dockerhub.yml to run with Dockerhub images
   ```bash
   docker compose up -f docker-compose-dockerhub.yml --build
   ```   

3. **Access the application**
   - **Web UI**: http://localhost:5173
   - **API**: http://localhost:5000
   - **API Health**: http://localhost:5000/api/health

4. **Login credentials**
   - Admin: `admin` / `admin`
   - Analyst: `analyst` / `analyst`

5. **Stop the application**
   ```bash
   docker compose down
   ```

## Manual Setup (Without Docker)

### Requirements

- Python 3.11+
- Node.js 20+
- SQLite 3
- Ollama (for AI features)

### Backend Setup

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt

# Run Flask API
python api/server.py
```

API will be available at http://localhost:5000

### Frontend Setup

```bash
# Navigate to UI folder
cd ui

# Install dependencies
npm install

# Run development server
npm run dev
```

UI will be available at http://localhost:5173

See [ui/README.md](ui/README.md) for UI-specific details and features.

### Ollama Setup

```bash
# Install Ollama
brew install ollama

# Start Ollama server
ollama serve
```
On another terminal:
This will produce a link. Follow the link and login with your credentails. This will create a Device API key automatically. 
```bash
ollama signin
```
Pull cloud model. 
```
ollama pull gpt-oss:120b-cloud
```

## Project Structure

```
.
├── api/                    # Flask REST API server
├── ai_engine/              # CrewAI-based analysis engine
│   └── cyber/              # Cyber security analysis crew
│       ├── agents.yaml     # Agent configurations
│       ├── tasks.yaml      # Task definitions
│       └── tools/          # Database query tools
├── data/                   # Raw data files
├── db/                     # SQLite database (cyber.db)
├── etl/                    # Data cleaning & transformation scripts
├── outputs/                # Processed data files
├── reports/                # Generated analysis reports
├── ui/                     # React frontend (see ui/README.md)
├── docker-compose.yml      # Docker orchestration
├── Dockerfile.api          # API container definition
└── Dockerfile.ui           # UI container definition
```

## Data Pipeline

1. **Extract** - Raw cyber security events data (2014-2025)
2. **Transform** - Clean and standardize using Python scripts in `etl/`
3. **Load** - Import into SQLite database (`db/cyber.db`)
4. **Analyze** - Query data via UI or AI analysis

### Running ETL Pipeline

```bash
python etl/01_load_to_db.py
```

## AI Analysis

The application uses CrewAI with Ollama's `gpt-oss:120b-cloud` model to answer questions about cyber security trends.

### Example Questions

- "What are the trends in cyber attacks from 2014 to 2024?"
- "Which countries experience the most state-sponsored attacks?"
- "What is the relationship between actor type and attack motive?"
- "Compare cyber incident patterns between NATO and non-NATO countries"

### API Endpoints

- `GET /api/health` - Health check
- `POST /api/analyze` - Submit analysis question
- `GET /api/examples` - Get example questions

## Dataset

- **Size**: 14,720 cyber security events
- **Time Range**: 2014-2025
- **Columns**: 34 attributes including:
  - Event details (date, type, actor, motive)
  - Target information (organization, industry, country)
  - Regional flags (NATO, EU, G7, G20, etc.)

## Development

### Hot Reload

Docker Compose is configured for development with hot reload:
- **API**: Python files auto-reload on changes
- **UI**: React components auto-reload on changes

### View Logs

```bash
docker compose logs -f          # All services
docker compose logs -f api      # API only
docker compose logs -f ui       # UI only
```

### Rebuild Containers

```bash
docker compose up --build
```

## Team

- Gabriel Zomignani
- Pawel Wlodarczyk
- Pablo Baena




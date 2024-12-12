# AnnotationEngine

[![codecov](https://codecov.io/gh/CAVEConnectome/AnnotationEngine/branch/master/graph/badge.svg)](https://codecov.io/gh/CAVEConnectome/AnnotationEngine)
[![GitHub Actions](https://github.com/CAVEConnectome/AnnotationEngine/actions/workflows/annotationengine.yml/badge.svg)](https://github.com/CAVEConnectome/AnnotationEngine/actions/workflows/annotationengine.yml)

## Overview

AnnotationEngine is a Flask-based service for managing annotations in a PostgreSQL/PostGIS database using the [DynamicAnnotationDB](https://github.com/CAVEconnectome/DynamicAnnotationDB/) library. It provides a REST API and web interface for creating, managing, and querying annotation tables with support for spatial annotation data and user permissions on given tables.

## Installation

### Local Installation

```bash
git clone https://github.com/CAVEConnectome/AnnotationEngine.git
cd AnnotationEngine
pip install -r requirements.txt
```

### Docker Setup

```bash
# start all the services
docker compose up

# Start the PostgreSQL database (for local db debugging)
docker compose up -d db

# Optional: Start adminer for database management
docker compose up -d adminer
```

## Configuration

### Environment Variables

Required environment variables (see .env.dev as an example):

```bash
FLASK_CONFIGURATION=development  # or production
POSTGRES_USER=postgres
POSTGRES_PASSWORD=yourpassword
POSTGRES_DB=annotation
AUTH_URI=your_auth_service
```

### Database Configuration

The service requires PostgreSQL with PostGIS extension. Configure the database connection in your environment or config file:

```python
SQLALCHEMY_DATABASE_URI = "postgresql://user:password@localhost:5432/annotation"
```

## Development

### Setup Development Environment

1. Install development requirements:

```bash
pip install -r dev_requirements.txt
```

2. Run tests:

```bash
# this will create a temporary postgis database for testing
pytest --docker=true
```

### Running the Server

Development server:

```bash
python run.py
```

The server will start on port 4001.

## API Usage

Full API documentation is available at `/annotation/api/doc` when the server is running.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

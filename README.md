# VB to Python FastAPI

A FastAPI application for processing Hong Kong stock exchange data with custom algorithms.

## Features

-   **Stock Algorithm Processing**: Processes HKEX stock data with custom algorithms
-   **REST API**: FastAPI-based REST endpoints for algorithm execution
-   **Database Integration**: MySQL database connectivity for stock price data
-   **Docker Support**: Containerized application with Docker Compose
-   **Logging**: Structured logging for monitoring and debugging

## API Endpoints

-   `GET /api/ping` - Health check endpoint
-   `POST /api/start-hk-algo` - Start Hong Kong stock algorithm processing

## Quick Start

### Using Docker Compose

```bash
docker-compose up -d
```

### Manual Setup

1. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

2. Configure database settings in `config/settings.py` or use environment variables

3. Run the application:
    ```bash
    python main.py
    ```

The application will be available at `http://localhost:8000`

## Configuration

Configure the application through environment variables or modify `config/settings.py`:

-   Database connection settings
-   CORS configuration
-   Logging levels
-   Application host and port

## Project Structure

-   `routes/` - API route definitions
-   `services/` - Business logic and algorithms
-   `config/` - Application configuration and logging
-   `logs/` - Application logs

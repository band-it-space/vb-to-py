# VB to Python FastAPI - Hong Kong Stock Technical Analysis API

A modern FastAPI application that provides technical analysis for Hong Kong Stock Exchange (HKEX) securities. This project represents a migration from Visual Basic to Python, offering real-time stock analysis with multiple data sources and advanced technical indicators.

## 🚀 Features

-   **Technical Analysis Indicators**
    -   RSI (Relative Strength Index) - 14-day period
    -   Moving averages for high/low prices (20D, 50D, 250D)
    -   Price Ratio (PR) calculations against HSI benchmark (5D, 20D, 60D, 125D, 250D)
-   **Dual Data Sources**
    -   MySQL database integration for historical data
    -   External HKEX API for real-time data validation
-   **RESTful API**
    -   FastAPI with automatic OpenAPI documentation
    -   CORS support for web applications
    -   Structured logging system
-   **Containerized Deployment**
    -   Docker support with hot reload
    -   Production-ready configuration

## 📋 Prerequisites

-   Python 3.8+
-   Docker & Docker Compose (optional)
-   MySQL database access
-   HKEX API access key

## 🛠️ Installation

### Option 1: Local Development

1. **Clone the repository**

    ```bash
    git clone <repository-url>
    cd vb-to-python-fastapi
    ```

2. **Install dependencies**

    ```bash
    pip install -r requirements.txt
    ```

3. **Configure environment variables**
   Create a `.env` file in the project root:

    ```env
    # Database Configuration
    SERHIO_DB_HOST=localhost
    SERHIO_DB_PORT=3306
    SERHIO_DB=derivates_crawler
    SERHIO_DB_USER=reader
    SERHIO_DB_PASS=your_password

    # API Configuration
    HOST=0.0.0.0
    PORT=8000
    DEBUG=True
    LOG_LEVEL=INFO
    ```

4. **Run the application**
    ```bash
    python main.py
    ```

### Option 2: Docker Deployment

1. **Build and run with Docker Compose**
    ```bash
    docker-compose up --build
    ```

The API will be available at `http://localhost:8000`

## 📚 API Documentation

### Endpoints

#### Health Check

```http
GET /api/ping
```

**Response:**

```json
{
    "status": "pong",
    "service": "fastapi-app"
}
```

#### Stock Technical Analysis

```http
POST /api/start-hk-ta
```

**Request Body:**

```json
{
    "stockname": "0700.HK",
    "tradeDay": "2024-01-15"
}
```

**Response:**

```json
{
    "status": "success",
    "stockname": "0700.HK",
    "tradeDay": "2024-01-15",
    "message": "Algorithm successfully completed for 0700.HK",
    "data_from_sergio_ta": {
        "high20": 285.5,
        "low20": 270.2,
        "high50": 295.8,
        "low50": 265.1,
        "high250": 320.4,
        "low250": 240.8,
        "pr5": 1.023,
        "pr20": 0.987,
        "pr60": 1.045,
        "pr125": 0.932,
        "pr250": 1.156,
        "rsi14": 62.34,
        "used_days_for_calculation": 250,
        "date_range": {
            "from": "2023-01-15",
            "to": "2024-01-15"
        }
    },
    "data_from_api_ta": {
        "high_20d": 285.5,
        "low_20d": 270.2,
        "rsi14": 62.34,
        "pr5": 1.023
    }
}
```

### Interactive API Documentation

Once the application is running, visit:

-   **Swagger UI**: `http://localhost:8000/docs`
-   **ReDoc**: `http://localhost:8000/redoc`

## 🏗️ Architecture

```
├── main.py                 # FastAPI application entry point
├── routes/
│   └── api_routes.py      # API endpoint definitions
├── services/
│   └── hk_ta.py          # Technical analysis algorithms
├── config/
│   ├── settings.py       # Application configuration
│   └── logger.py         # Logging configuration
├── logs/                 # Application logs
├── requirements.txt      # Python dependencies
├── Dockerfile           # Container configuration
└── docker-compose.yml   # Docker orchestration
```

## 📊 Technical Indicators Explained

### RSI (Relative Strength Index)

-   **Period**: 14 days
-   **Range**: 0-100
-   **Interpretation**: Values above 70 indicate overbought conditions, below 30 indicate oversold

### Moving Averages (High/Low)

-   **20D**: Short-term trend analysis
-   **50D**: Medium-term trend analysis
-   **250D**: Long-term trend analysis (approximately 1 year)

### Price Ratio (PR)

Compares stock performance against HSI benchmark (2800.HK):

-   **PR > 1.0**: Stock outperforming benchmark
-   **PR < 1.0**: Stock underperforming benchmark
-   **Periods**: 5D, 20D, 60D, 125D, 250D

## 🔧 Configuration

The application uses Pydantic settings with environment variable support:

| Variable         | Default           | Description       |
| ---------------- | ----------------- | ----------------- |
| `HOST`           | 0.0.0.0           | Server host       |
| `PORT`           | 8000              | Server port       |
| `DEBUG`          | True              | Debug mode        |
| `LOG_LEVEL`      | INFO              | Logging level     |
| `SERHIO_DB_HOST` | localhost         | Database host     |
| `SERHIO_DB_PORT` | 3306              | Database port     |
| `SERHIO_DB`      | derivates_crawler | Database name     |
| `SERHIO_DB_USER` | reader            | Database user     |
| `SERHIO_DB_PASS` | password          | Database password |

## 📝 Usage Examples

### Using cURL

```bash
# Health check
curl -X GET "http://localhost:8000/api/ping"

# Technical analysis
curl -X POST "http://localhost:8000/api/start-hk-ta" \
  -H "Content-Type: application/json" \
  -d '{
    "stockname": "0700.HK",
    "tradeDay": "2024-01-15"
  }'
```

### Using Python requests

```python
import requests

# Health check
response = requests.get("http://localhost:8000/api/ping")
print(response.json())

# Technical analysis
payload = {
    "stockname": "0700.HK",
    "tradeDay": "2024-01-15"
}
response = requests.post("http://localhost:8000/api/start-hk-ta", json=payload)
print(response.json())
```

## 🔍 Data Sources

1. **MySQL Database** (`hkex_stock_price` table)

    - Historical stock data
    - Up to 500 days of trading history
    - Excludes weekends automatically

2. **External HKEX API**
    - Real-time data validation
    - Backup data source
    - API endpoint: `http://ete.stockfisher.com.hk/v1.1/debugHKEX/verifyData`

## 📊 Logging

The application provides comprehensive logging:

-   **fastapi_app.log**: Main application events
-   **api_routes.log**: API request/response logging
-   **algorithm_handler.log**: Technical analysis processing

## 🚀 Deployment

### Production Deployment

1. **Set production environment variables**
2. **Configure reverse proxy (nginx)**
3. **Set up SSL certificates**
4. **Configure database connection pooling**

### Docker Production

```bash
# Build production image
docker build -t hk-stock-api:latest .

# Run with production settings
docker run -d \
  --name hk-stock-api \
  -p 8000:8000 \
  -e DEBUG=False \
  -e LOG_LEVEL=WARNING \
  hk-stock-api:latest
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🐛 Troubleshooting

### Common Issues

**Database Connection Error**

-   Verify database credentials in `.env` file
-   Ensure MySQL server is running and accessible
-   Check firewall settings

**API Timeout**

-   External API may be slow or unavailable
-   Check network connectivity
-   Verify API key is valid

**Missing Data**

-   Stock code format should include `.HK` suffix
-   Trade day should be a valid trading day (weekday)
-   Ensure sufficient historical data exists

## 📞 Support

For issues and questions:

1. Check the logs in the `logs/` directory
2. Review the API documentation at `/docs`
3. Create an issue in the repository

---

**Migration Note**: This project represents a successful migration from Visual Basic to modern Python FastAPI, maintaining all original functionality while adding improved performance, scalability, and maintainability.

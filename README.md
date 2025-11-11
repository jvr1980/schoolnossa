# School Nossa - Berlin School Selection Dashboard

A data-driven dashboard to help parents make informed decisions about school selection in Berlin.

## Features

- 📊 Historical school performance tracking (year-over-year comparisons)
- 🗺️ School locations and enrollment areas
- 📈 Performance metrics (Abitur success rates, average grades)
- 👥 Student/teacher ratios and demographic data
- 🔍 Search and filter schools by district, type, and performance

## Data Sources

- **Berlin Open Data Portal**: Official school data (locations, types, enrollment areas)
- **sekundarschulen-berlin.de**: Performance metrics (future)

## Tech Stack

- **Backend**: Python 3.11+, FastAPI
- **Database**: PostgreSQL 15+
- **Data Processing**: Pandas
- **Frontend**: Next.js 14+ (planned)

## Project Structure

```
schoolnossa/
├── src/
│   ├── api/              # FastAPI application
│   ├── collectors/       # Data collection scripts
│   ├── models/           # SQLAlchemy models
│   ├── services/         # Business logic
│   └── utils/            # Utilities
├── scripts/              # Management scripts
├── tests/                # Test suite
├── alembic/              # Database migrations
└── frontend/             # Next.js app (future)
```

## Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 15+

### Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd schoolnossa
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Initialize database:
```bash
python scripts/init_db.py
```

6. Collect initial data:
```bash
python scripts/collect_berlin_data.py
```

## Development

### Run API server:
```bash
uvicorn src.api.main:app --reload
```

### Run data collection:
```bash
python scripts/collect_berlin_data.py
```

### Run tests:
```bash
pytest
```

## Database Schema

See [SCHOOL_DATA_INVESTIGATION.md](./SCHOOL_DATA_INVESTIGATION.md) for detailed schema documentation.

## Roadmap

- [x] Investigation and requirements gathering
- [x] Database schema design
- [ ] POC: Berlin Open Data API integration
- [ ] Historical data tracking and year-over-year comparisons
- [ ] Web scraping for performance metrics
- [ ] REST API endpoints
- [ ] Frontend dashboard
- [ ] Semantic search with vector embeddings

## Contributing

This is currently a personal project. Contributions and suggestions are welcome!

## License

TBD

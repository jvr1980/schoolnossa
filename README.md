# SchoolNossa

A comprehensive data platform for German secondary schools, enabling comparison across cities using standardized metrics including academic performance, demographics, accessibility, and neighborhood context.

## Quick Start

```bash
# Install dependencies
pip install -r requirements_crawler.txt

# Configure API keys in config.yaml
cp .env.example .env

# Run Berlin pipeline (dry run)
python3 scripts_berlin/Berlin_secondary_school_data_asset_builder_orchestrator.py --dry-run

# Run Berlin pipeline (full)
python3 scripts_berlin/Berlin_secondary_school_data_asset_builder_orchestrator.py
```

## Project Structure

```
schoolnossa/
├── scripts_shared/          # Reusable scripts across all cities
│   ├── enrichment/          # POI, tuition parsing
│   ├── generation/          # LLM descriptions, metadata extraction
│   ├── processing/          # Vector DB preparation
│   └── utils/               # Google Places, common utilities
│
├── scripts_berlin/          # Berlin-specific scripts
│   ├── scrapers/            # Data collection from Berlin sources
│   ├── enrichment/          # Berlin-specific enrichment (traffic, transit, MSA)
│   ├── processing/          # Data combination and merging
│   └── Berlin_*.py          # Berlin pipeline orchestrator
│
├── scripts_hamburg/         # Hamburg-specific scripts (planned)
│
├── data_berlin/             # Berlin data files
│   ├── raw/                 # Initial scraped data
│   ├── intermediate/        # Processing stages
│   ├── final/               # Final outputs (parquet, CSV)
│   └── descriptions/        # Generated school descriptions
│
├── data_hamburg/            # Hamburg data files (planned)
│
├── docs/                    # Documentation
│   ├── PROJECT_ARCHITECTURE.md  # Detailed architecture guide
│   └── HAMBURG_IMPLEMENTATION_PLAN.md
│
├── archive/                 # Deprecated scripts and data
└── logs/                    # Pipeline execution logs
```

## Documentation

- **[PROJECT_ARCHITECTURE.md](docs/PROJECT_ARCHITECTURE.md)** - Complete architecture, data schema, and city implementation guide
- **[HAMBURG_IMPLEMENTATION_PLAN.md](docs/HAMBURG_IMPLEMENTATION_PLAN.md)** - Plan for Hamburg expansion

## Supported Cities

| City | Status | Schools | Final Output |
|------|--------|---------|--------------|
| Berlin | Complete | 259 | `data_berlin/final/school_master_table_final_with_embeddings.parquet` |
| Hamburg | Planned | - | - |

## Key Features

- **Multi-source data aggregation**: School portals, academic statistics, crime data, traffic sensors
- **AI-enhanced descriptions**: LLM-generated rich descriptions using GPT-4, Gemini, Perplexity
- **Vector embeddings**: Semantic search and similarity matching using OpenAI embeddings
- **Extensible architecture**: City-specific scrapers with shared enrichment pipeline

## License

Private/Internal Use

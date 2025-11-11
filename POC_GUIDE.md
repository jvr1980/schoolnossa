# School Nossa - Proof of Concept Guide

This POC demonstrates the core functionality of the Berlin School Selection Dashboard, focusing on data collection from the Berlin Open Data Portal and year-over-year comparison capabilities.

## What's Included in This POC

✅ **Database Schema** - Designed for historical tracking with year-over-year comparisons
✅ **Berlin Open Data Collector** - Fetches school data from official WFS endpoint
✅ **School Service** - Business logic with year-over-year calculation methods
✅ **FastAPI REST API** - Endpoints for querying school data
✅ **Data Collection Scripts** - Automated data import tools

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Berlin Open Data Portal                   │
│              (WFS Endpoint + CKAN API)                       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ HTTPS
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Data Collector (Python + httpx)                 │
│  - Fetches school data from WFS                              │
│  - Parses GeoJSON features                                   │
│  - Normalizes data to schema                                 │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│           School Service (Business Logic)                    │
│  - Import/update schools                                     │
│  - Calculate year-over-year changes                          │
│  - Query historical metrics                                  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              PostgreSQL Database                             │
│  Tables:                                                     │
│  - schools (core info)                                       │
│  - school_metrics_annual (historical data)                   │
│  - enrollment_areas (geospatial)                             │
│  - collection_log (audit trail)                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                 FastAPI REST API                             │
│  Endpoints:                                                  │
│  - GET /schools (list with filters)                          │
│  - GET /schools/{id} (details)                               │
│  - GET /schools/{id}/metrics (historical)                    │
│  - GET /districts (filter options)                           │
│  - POST /admin/import (trigger collection)                   │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Prerequisites

```bash
# Python 3.11+
python --version

# PostgreSQL 15+
psql --version
```

### 2. Install Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install packages
pip install -r requirements.txt
```

### 3. Configure Database

```bash
# Copy environment template
cp .env.example .env

# Edit .env and set your database credentials
# DATABASE_URL=postgresql://user:password@localhost:5432/schoolnossa
```

### 4. Initialize Database

```bash
# Create database
createdb schoolnossa

# Initialize tables
python scripts/init_db.py
```

### 5. Collect Data

```bash
# Fetch schools from Berlin Open Data Portal
python scripts/collect_berlin_data.py
```

### 6. Start API Server

```bash
# Run development server
uvicorn src.api.main:app --reload

# API will be available at:
# - http://localhost:8000
# - Docs: http://localhost:8000/docs
```

## Testing the POC

### Using the API Documentation

1. Open browser: http://localhost:8000/docs
2. Try these endpoints:

**Get all schools:**
```
GET /schools?limit=10
```

**Get schools in a district:**
```
GET /schools?district=Mitte&limit=20
```

**Get school details:**
```
GET /schools/{school_id}
```

**Get districts and types:**
```
GET /districts
```

### Using cURL

```bash
# Get list of schools
curl http://localhost:8000/schools?limit=5

# Get specific school
curl http://localhost:8000/schools/{school_id}

# Get school metrics (historical)
curl http://localhost:8000/schools/{school_id}/metrics?years=3

# Get filter options
curl http://localhost:8000/districts

# Trigger data import
curl -X POST http://localhost:8000/admin/import
```

### Using Python

```python
import requests

# Get schools
response = requests.get("http://localhost:8000/schools", params={"limit": 10})
schools = response.json()

print(f"Found {len(schools)} schools")
for school in schools:
    print(f"- {school['name']} ({school['school_type']}) in {school['district']}")
```

## Key Features Demonstrated

### 1. Historical Data Model

The database schema is designed to track changes over time:

```sql
-- Each school has one row per year in school_metrics_annual
school_metrics_annual:
  - school_id: "12345"
  - year: 2024
  - total_students: 850
  - students_change_percent: +5.2  -- 5.2% increase from 2023
  - student_teacher_ratio: 16.5
  - ratio_change_percent: -2.1      -- Improved (decreased) by 2.1%
```

### 2. Year-over-Year Calculations

The `SchoolService` class provides methods to calculate changes:

```python
# Calculate % change for any metric
service.calculate_year_over_year_change(
    school_id="12345",
    current_year=2024,
    metric="total_students"
)
# Returns: 5.2 (meaning 5.2% increase)

# Get historical trends
metrics = service.get_school_metrics_history(
    school_id="12345",
    years=3
)
# Returns: [2024_data, 2023_data, 2022_data]
```

### 3. Multi-Year Comparisons

The schema supports comparing any two years:

```python
# Compare 2024 vs 2023
# Compare 2023 vs 2022
# Compare 2024 vs 2022 (2-year trend)
```

This addresses your requirement to show:
- "% change from this year to last year"
- "% change from 2 years ago to 1 year ago"

## What's NOT in This POC (Yet)

⏭️ **Performance Metrics Scraping** - sekundarschulen-berlin.de data collection
⏭️ **Frontend Dashboard** - React/Next.js UI
⏭️ **Actual Historical Data** - Multiple years of snapshots
⏭️ **Semantic Search** - Chroma vector database integration
⏭️ **Authentication** - Admin endpoints are unprotected
⏭️ **Deployment** - Production configuration

## Next Steps

### Phase 1: Enrich Data (2-3 weeks)
- Build web scraper for sekundarschulen-berlin.de
- Collect Abitur success rates, grades, demand metrics
- Populate `school_metrics_annual` with performance data
- Link scraped data to schools using name/address matching

### Phase 2: Historical Collection (1 week)
- Scrape historical data from previous years
- Populate multiple years in `school_metrics_annual`
- Calculate and store year-over-year changes

### Phase 3: Frontend Dashboard (3-4 weeks)
- Build Next.js application
- School search and filtering
- Comparison tool
- Trend visualizations (charts showing year-over-year changes)
- Map view with school locations

### Phase 4: Enhanced Features (2-3 weeks)
- Semantic search with Chroma
- School recommendations based on preferences
- Export functionality (PDF reports)
- Email alerts for data updates

## Database Schema Highlights

### Schools Table
```sql
schools:
  - school_id (unique identifier)
  - name, school_type, address
  - district (for filtering)
  - latitude, longitude (for map)
  - contact_info (JSON)
```

### School Metrics Annual Table
```sql
school_metrics_annual:
  - school_id + year (composite key)
  - total_students + students_change_percent
  - total_teachers + teachers_change_percent
  - student_teacher_ratio + ratio_change_percent
  - abitur_success_rate + abitur_success_change_percent
  - abitur_average_grade + abitur_grade_change
  - demand_score + demand_change_percent
  - raw_data (JSON for flexibility)
```

**Key Design Decision**: Store both raw values AND pre-calculated percentages for:
- Fast queries (no need to join multiple years for simple displays)
- Consistent calculations (% is calculated once during import)
- Historical accuracy (even if calculation logic changes)

## Example Parent Use Case

**Scenario**: Parent looking for a Gymnasium in Mitte with improving performance

1. **Browse schools**:
   ```
   GET /schools?district=Mitte&school_type=Gymnasium
   ```

2. **View specific school**:
   ```
   GET /schools/12345
   ```

3. **Check trends**:
   ```
   GET /schools/12345/metrics?years=3
   ```

4. **Analyze the data**:
   - 2024: 850 students (+5.2%), 92% Abitur success (+3.1%), avg grade 2.4 (-0.2 improvement)
   - 2023: 807 students (+2.1%), 89% Abitur success (+1.5%), avg grade 2.6
   - 2022: 790 students, 88% Abitur success, avg grade 2.7

**Insight**: This school is growing, with improving Abitur success rates and grades!

## Technical Decisions

### Why PostgreSQL?
- ✅ Excellent support for time-series data
- ✅ JSONB for flexible data storage
- ✅ Strong query performance
- ✅ PostGIS extension available for geospatial queries
- ✅ Industry standard, well-supported

### Why FastAPI?
- ✅ Modern, fast Python framework
- ✅ Automatic API documentation (OpenAPI/Swagger)
- ✅ Type hints and validation (Pydantic)
- ✅ Async support for scalability
- ✅ Easy to learn and maintain

### Why SQLAlchemy?
- ✅ Powerful ORM with good performance
- ✅ Database-agnostic (can switch DBs if needed)
- ✅ Migration support via Alembic
- ✅ Excellent for complex queries

### Why httpx over requests?
- ✅ Async/await support
- ✅ HTTP/2 support
- ✅ Modern API
- ✅ Better timeout handling

## Troubleshooting

### Database Connection Error
```
Error: could not connect to server
```
**Solution**: Check PostgreSQL is running and .env has correct credentials

### Import Returns 0 Schools
```
Successfully parsed 0 schools
```
**Solution**: WFS endpoint may be down or field names changed. Check logs.

### 403 Forbidden from WFS
```
HTTP error: 403
```
**Solution**: WFS might block automated requests. Try:
- Adding user agent header
- Rate limiting requests
- Using VPN if IP is blocked

### Module Not Found
```
ModuleNotFoundError: No module named 'src'
```
**Solution**: Run scripts from project root: `python scripts/init_db.py`

## API Endpoints Reference

| Endpoint | Method | Description | Parameters |
|----------|--------|-------------|------------|
| `/` | GET | Root endpoint | - |
| `/health` | GET | Health check | - |
| `/schools` | GET | List schools | district, school_type, limit, offset |
| `/schools/{id}` | GET | School details | school_id |
| `/schools/{id}/metrics` | GET | Historical metrics | school_id, years |
| `/districts` | GET | Filter options | - |
| `/admin/import` | POST | Import data | - |

## Performance Considerations

### Current Design (POC)
- ✅ Simple, straightforward queries
- ✅ Pre-calculated percentages stored
- ⚠️ No caching
- ⚠️ No pagination on metrics
- ⚠️ Synchronous data collection

### Production Recommendations
- Add Redis caching for frequently accessed schools
- Implement background jobs for data collection (Celery/RQ)
- Add database connection pooling
- Optimize queries with proper indexing
- Add rate limiting to API
- Implement full-text search (PostgreSQL FTS or Elasticsearch)

## Data Collection Strategy

### Initial Collection
1. Fetch all schools from Berlin Open Data WFS
2. Store in `schools` table
3. Manual or scripted collection of performance metrics
4. Store in `school_metrics_annual` for current year

### Annual Updates
1. Run collection script once per year (e.g., September)
2. Collect new year's data
3. Calculate year-over-year changes automatically
4. Old data persists for historical analysis

### Incremental Updates
- Basic info (schools table): Quarterly updates sufficient
- Metrics (school_metrics_annual): Annual updates sufficient
- Performance data: Can be updated more frequently if available

## Extending the POC

### Add New Metrics

1. Update schema in `src/models/school.py`:
```python
# Add to SchoolMetricsAnnual
special_programs_count = Column(Integer)
special_programs_change_percent = Column(DECIMAL(5, 2))
```

2. Update API schema in `src/api/schemas.py`:
```python
# Add to SchoolMetricsResponse
special_programs_count: Optional[int] = None
special_programs_change_percent: Optional[Decimal] = None
```

3. Update data collection to populate new fields

### Add New Data Sources

1. Create new collector in `src/collectors/`:
```python
class SekundarschulenBerlinCollector:
    async def collect_performance_metrics(self):
        # Scraping logic here
```

2. Update `SchoolService` to use new collector

3. Add data source field to track origin

## License & Data Attribution

**Berlin Open Data**: Licensed under CC BY 3.0 DE
- Must provide attribution to: Senatsverwaltung für Bildung, Jugend und Familie
- Data source must be cited on dashboard

**sekundarschulen-berlin.de**: Check robots.txt and ToS before scraping
- Respect rate limits
- Provide attribution
- Contact website owner if collecting large amounts of data

## Contact & Support

For questions about this POC:
- Review the main investigation document: `SCHOOL_DATA_INVESTIGATION.md`
- Check API documentation: http://localhost:8000/docs
- Review code comments in source files

## Summary

This POC successfully demonstrates:

✅ **Data Collection** - Automated import from Berlin Open Data Portal
✅ **Historical Schema** - Database designed for year-over-year tracking
✅ **Year-over-Year Logic** - Methods to calculate percentage changes
✅ **REST API** - Query interface for frontend consumption
✅ **Scalable Architecture** - Clean separation of concerns
✅ **Parent-Focused** - Data structure optimized for school selection decisions

**Ready for**: Frontend development, additional data sources, deployment

**Next priorities**:
1. Collect actual performance metrics from sekundarschulen-berlin.de
2. Populate historical data (multiple years)
3. Build React/Next.js dashboard
4. Deploy to production

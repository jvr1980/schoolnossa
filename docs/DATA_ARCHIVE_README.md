# Data Archive Feature

All crawling scripts now include an automatic data archiving feature that creates timestamped backups of all crawled data.

## How It Works

Each time you run a crawler script, it automatically:

1. **Creates a timestamped archive directory**:
   - Format: `data_archive/<source>_YYYYMMDD_HHMMSS/`
   - Example: `data_archive/bildung_berlin_20251116_155030/`

2. **Saves all crawled data to the archive**:
   - Individual school/kita JSON files
   - Manifest file with crawl statistics
   - Complete backup of the entire run

3. **Also uploads to GCS** (if configured):
   - Main location: Latest data
   - Archive location: `data_archive/<timestamp>/` in GCS bucket

## Scripts with Archive Feature

### 1. crawl_school_bildungberlin.py
- **Local archive**: `data_archive/bildung_berlin_YYYYMMDD_HHMMSS/`
- **GCS archive**: `bildung_berlin/schools/` (main) + `data_archive/bildung_berlin_YYYYMMDD_HHMMSS/` (archive)
- **Data**: 1,419 schools from bildung.berlin.de

### 2. crawl_schools_sekundarschule-berlin.py
- **Local archive**: `data_archive/sekundarschulen_YYYYMMDD_HHMMSS/`
- **GCS archive**: `schools/` (main) + `data_archive/sekundarschulen_YYYYMMDD_HHMMSS/` (archive)
- **Data**: Schools from sekundarschulen-berlin.de sitemap

### 3. crawl_kita_navigator.py
- **Local archive**: `data_archive/kita_navigator_YYYYMMDD_HHMMSS/`
- **GCS archive**: `kitas/` (main) + archive location
- **Data**: Kitas from kita-navigator.berlin.de

## Benefits

1. **Version History**: Every run creates a new timestamped archive
2. **Easy Rollback**: Can revert to any previous crawl by timestamp
3. **Audit Trail**: Complete history of all crawls with timestamps
4. **Disaster Recovery**: Local and cloud backups for redundancy
5. **Data Comparison**: Compare data between different crawl times

## Directory Structure

```
schoolnossa/
├── data_archive/
│   ├── bildung_berlin_20251116_155030/
│   │   ├── bildung_berlin_schools_28981_GPB_College_gGmbH_20251116.json
│   │   ├── bildung_berlin_schools_28982_GPB_College_gGmbH_20251116.json
│   │   ├── ...
│   │   └── manifest.json
│   ├── sekundarschulen_20251116_160000/
│   │   ├── schools_...json
│   │   └── manifest.json
│   └── kita_navigator_20251116_170000/
│       ├── kitas_...json
│       └── manifest.json
├── crawled_schools/  (latest backup)
└── crawled_kitas/    (latest backup)
```

## Manifest File

Each archive includes a `manifest.json` file with:

```json
{
  "crawl_date": "2025-11-16T15:50:30.123456",
  "run_timestamp": "20251116_155030",
  "total_schools": 1419,
  "successfully_crawled": 1419,
  "base_url": "https://...",
  "schools": [...]
}
```

## Storage Locations

### Local Storage
- **Current/Latest**: `crawled_schools/` or `crawled_kitas/`
- **Archives**: `data_archive/<source>_<timestamp>/`

### Google Cloud Storage
- **Current/Latest**: Root of respective folder (e.g., `bildung_berlin/schools/`)
- **Archives**: `data_archive/<source>_<timestamp>/`

## Restoring from Archive

To restore data from a specific archive:

1. **Find the desired timestamp**:
   ```bash
   ls data_archive/
   ```

2. **Copy files from archive**:
   ```bash
   cp -r data_archive/bildung_berlin_20251116_155030/* crawled_schools/
   ```

3. **Or use the archive directly** for analysis:
   ```python
   import json
   from pathlib import Path

   archive_dir = Path("data_archive/bildung_berlin_20251116_155030")
   with open(archive_dir / "manifest.json") as f:
       manifest = json.load(f)
   ```

## Cleanup Old Archives

Archive directories can grow large over time. To manage storage:

```bash
# Remove archives older than 30 days
find data_archive -type d -name "*_*" -mtime +30 -exec rm -rf {} \;

# Or keep only the last N archives
ls -t data_archive/ | tail -n +11 | xargs -I {} rm -rf data_archive/{}
```

## Configuration

The archive feature is enabled by default. To disable:

```python
# For bildung_berlin crawler
crawler = BildungBerlinCrawler(
    ...
    local_backup=False  # Disables local backup only, archive still created
)

# For kita_navigator crawler
crawler = KitaNavigatorCrawler(
    ...
    enable_archive=False  # Disables archive completely
)
```

## Notes

- Archives are never automatically deleted
- Each archive is completely independent
- Archive directory names include the exact time the script was started
- Both local and GCS archives use the same timestamp
- The manifest file can be used to verify data completeness

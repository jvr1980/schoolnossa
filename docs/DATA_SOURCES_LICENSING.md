# Data Sources Licensing & Terms of Use - Berlin

This document summarizes the licensing terms, commercial use permissions, and attribution requirements for all data sources used in the Berlin school data pipeline.

---

## Summary Table

| Data Source | License | Commercial Use | Attribution Required | Risk Level |
|-------------|---------|----------------|---------------------|------------|
| bildung.berlin.de (Schulporträt) | CC-BY 4.0 | **YES** | Yes - "Senatsverwaltung für Bildung, Jugend und Familie" | Low |
| daten.berlin.de (Open Data Portal) | CC-BY 4.0 / DL-DE 2.0 | **YES** | Yes - Per dataset | Low |
| Kriminalitätsatlas Berlin | CC-BY-SA | **YES** | Yes - "Polizei Berlin" | Low |
| VBB Transit Data (GTFS) | CC-BY 3.0 | **YES** | Yes - "VBB Verkehrsverbund Berlin-Brandenburg GmbH" | Low |
| Telraam Traffic Data | CC-BY-NC | **NO** (without permission) | Yes - "Telraam" | **High** |
| sekundarschulen-berlin.de | No explicit license | **Unclear** | Unknown | **Medium** |
| Google Places API | Proprietary | **YES** (with restrictions) | Yes - Google logo + providers | **Medium** |

---

## Detailed Analysis by Source

### 1. bildung.berlin.de - Schulporträt (School Portal)

**Source**: https://www.bildung.berlin.de/Schulverzeichnis/

**Operator**: Senatsverwaltung für Bildung, Jugend und Familie (Senate Department for Education, Youth and Family)

**License**: [Creative Commons Attribution 4.0 (CC-BY 4.0)](https://creativecommons.org/licenses/by/4.0/)

**Commercial Use**: **PERMITTED**
> "Die Datennutzung ist entgeltfrei unter Nennung der Datenquelle sowohl für nicht-kommerzielle wie kommerzielle Zwecke zulässig."
> (Data use is free of charge with source attribution for both non-commercial and commercial purposes.)

**Attribution Required**: Yes
- Must cite: "Senatsverwaltung für Bildung, Jugend und Familie"

**Legal Basis**:
- Berlin E-Government Act § 13
- Open Data Regulation (OpenDataV) of July 24, 2020

**Data Covered**:
- School statistics (student/teacher counts)
- School profiles and contact information
- Academic performance data

**Contact**: open-data@senbjf.berlin.de

**Source**: [berlin.de/sen/bildung/service/daten/](https://www.berlin.de/sen/bildung/service/daten/)

---

### 2. daten.berlin.de - Berlin Open Data Portal

**Source**: https://daten.berlin.de

**Operator**: Senatskanzlei Berlin / BerlinOnline

**Licenses Used**:
- [Creative Commons Attribution 4.0 (CC-BY 4.0)](https://creativecommons.org/licenses/by/4.0/)
- [Datenlizenz Deutschland Namensnennung 2.0](https://www.govdata.de/dl-de/by-2-0)
- [Datenlizenz Deutschland Zero 2.0](https://www.govdata.de/dl-de/zero-2-0)

**Commercial Use**: **PERMITTED** (for most datasets)
> Per Berlin E-Government Act § 13, data must be provided for both commercial and non-commercial use.

**Key Points**:
- Individual dataset licenses are specified in metadata
- Most datasets use CC-BY or DL-DE Namensnennung (attribution required)
- Some datasets may have additional restrictions

**How to Check License**:
Each dataset page on daten.berlin.de displays its specific license in the metadata section.

**Source**: [daten.berlin.de](https://daten.berlin.de)

---

### 3. Kriminalitätsatlas Berlin (Crime Statistics)

**Source**: https://www.kriminalitaetsatlas.berlin.de/

**Operator**: Polizei Berlin (Berlin Police)

**License**: [Creative Commons Attribution Share-Alike (CC-BY-SA)](https://creativecommons.org/licenses/by-sa/4.0/)

**Commercial Use**: **PERMITTED**
- Must attribute source
- Derivative works must use same license (share-alike)

**Attribution Required**: Yes
- Must cite: "Polizei Berlin"

**Data Covered**:
- Crime statistics by district (Bezirk) and neighborhood (Bezirksregion)
- 17 crime categories
- Historical data 2015-2024

**Important Note**: CC-BY-SA requires that any derived works (including your enriched database) must also be shared under the same license if distributed publicly.

**Source**: [daten.berlin.de/datensaetze/kriminalitatsatlas-berlin](https://daten.berlin.de/datensaetze/kriminalitatsatlas-berlin)

---

### 4. VBB Transit Data (BVG/S-Bahn)

**Source**: https://www.vbb.de/unsere-themen/vbbdigital/api-entwicklerinfos/datensaetze

**Operator**: Verkehrsverbund Berlin-Brandenburg GmbH (VBB)

**License**: [Creative Commons Attribution 3.0 (CC-BY 3.0)](https://creativecommons.org/licenses/by/3.0/)

**Commercial Use**: **PERMITTED**

**Attribution Required**: Yes
- Must cite: "VBB Verkehrsverbund Berlin-Brandenburg GmbH"
- Logos available at: https://www.vbb.de/presse/media-service/logos

**Data Covered**:
- GTFS timetable data (bus, train, tram, ferry)
- Stop locations and routes
- Real-time data (via API with registration)

**API Access**:
- GTFS static data: Freely downloadable
- API access: Requires registration at api@vbb.de
- Commercial API use may require separate agreement

**Source**: [daten.berlin.de/datensaetze/vbb-fahrplandaten-via-gtfs](https://daten.berlin.de/datensaetze/vbb-fahrplandaten-via-gtfs)

---

### 5. Telraam Traffic Data

**Source**: https://telraam.net / https://berlin-zaehlt.de

**Operator**: Telraam (Transport & Mobility Leuven, Belgium)

**License**: [Creative Commons Attribution-NonCommercial (CC-BY-NC)](https://creativecommons.org/licenses/by-nc/4.0/)

**Commercial Use**: **NOT PERMITTED** (without explicit agreement)

> "All Telraam data are available under a CC-BY-NC license. That means you can use, adapt and publish data freely for non-commercial purposes."

**However**:
> "We invite everyone to explore commercial or non-commercial opportunities with Telraam data; we only want our efforts and those of our participating citizens to be rewarded fairly if there's a commercial success."

**Commercial License**:
- Contact: info@telraam.net
- Commercial use requires negotiation and likely payment
- Data subscriptions available for professional/commercial use

**Attribution Required**: Yes
- Must cite: "Telraam"

**Data Covered**:
- Vehicle counts (cars, bikes, pedestrians, heavy vehicles)
- Speed data (V85)
- Hourly/daily aggregations

**Risk Assessment**: **HIGH RISK** for commercial use without explicit agreement

**Source**: [faq.telraam.net/article/9/telraam-data-license](https://faq.telraam.net/article/9/telraam-data-license-what-can-i-do-with-the-telraam-data)

---

### 6. sekundarschulen-berlin.de

**Source**: https://www.sekundarschulen-berlin.de

**Operator**: René Meintz (private individual)
- Address: Gärtnerstr. 20, 10245 Berlin
- Email: schulverzeichnis@online.de

**License**: **No explicit license stated**

**Commercial Use**: **UNCLEAR**

The website provides:
- A disclaimer about accuracy of information
- No explicit terms of use for data
- No stated license for content

**Data Covered**:
- School listings and basic information
- Links to school websites
- District organization

**Risk Assessment**: **MEDIUM RISK**
- This is a private website, not an official government source
- No explicit permission for commercial data use
- Should contact operator for commercial licensing

**Recommendation**:
- Contact schulverzeichnis@online.de for commercial use permission
- Or replace with official bildung.berlin.de data (which is CC-BY licensed)

**Source**: [sekundarschulen-berlin.de/impressum](https://www.sekundarschulen-berlin.de/impressum)

---

### 7. Google Places API (POI Data)

**Source**: Google Maps Platform

**License**: Proprietary (Google Maps Platform Terms of Service)

**Commercial Use**: **PERMITTED with significant restrictions**

**Key Restrictions**:

1. **No Caching/Storage**:
   > "You must not pre-fetch, cache, or store any Content except under the limited conditions stated in the Terms."

2. **Exception - Place IDs Only**:
   > "The place ID is exempt from caching restrictions. You can store place ID values indefinitely."

3. **Display Requirements**:
   - Results on a map must use Google Maps
   - Google logo and attributions required
   - Third-party data provider attributions required

4. **Prohibited Uses**:
   - Cannot charge users for API access
   - Cannot use for asset tracking (fleet management) without Premium license
   - Cannot scrape or persist data beyond user sessions

**What You CAN Store**:
- Place IDs (permanently)
- Nothing else from the API response

**What You CANNOT Store**:
- Place names
- Addresses
- Reviews
- Photos
- Ratings
- Any other content

**Attribution Required**: Yes
- Google logo
- Third-party data providers (shown in API response)

**Cost**: Pay-per-use pricing
- $17 per 1000 requests (Place Details)
- $32 per 1000 requests (Nearby Search)
- Free $200/month credit

**Risk Assessment**: **MEDIUM RISK**
- Commercial use allowed but with strict caching limitations
- Storing POI details (names, addresses) violates ToS
- Only Place IDs can be stored permanently

**Recommendation**:
- Store only Place IDs in database
- Fetch fresh data from API when displaying to users
- Or use alternative like OpenStreetMap (ODbL license, allows caching)

**Source**: [developers.google.com/maps/documentation/places/web-service/policies](https://developers.google.com/maps/documentation/places/web-service/policies)

---

## Recommendations for Commercial Use

### Green Light (Low Risk)
These sources are safe for commercial use with proper attribution:

1. **bildung.berlin.de** - CC-BY 4.0, explicitly permits commercial use
2. **daten.berlin.de** - Most datasets CC-BY, verify per dataset
3. **VBB Transit Data** - CC-BY 3.0, commercial use permitted

### Yellow Light (Medium Risk)
Proceed with caution:

1. **Kriminalitätsatlas Berlin** - CC-BY-SA requires share-alike (your derived database must also be open)
2. **sekundarschulen-berlin.de** - No license stated, contact operator
3. **Google Places API** - Commercial use OK but cannot cache data (only Place IDs)

### Red Light (High Risk)
Requires explicit commercial license:

1. **Telraam Traffic Data** - CC-BY-NC explicitly prohibits commercial use without agreement

---

## Required Attributions Summary

If using this data commercially, include the following attributions:

```
Data Sources:
- School data: Senatsverwaltung für Bildung, Jugend und Familie (CC-BY 4.0)
- Crime statistics: Polizei Berlin (CC-BY-SA)
- Transit data: VBB Verkehrsverbund Berlin-Brandenburg GmbH (CC-BY 3.0)
- Traffic data: Telraam (requires commercial license for commercial use)
- Points of Interest: Google Maps Platform
```

---

## Action Items for Commercial Deployment

1. **Telraam**: Contact info@telraam.net to negotiate commercial license or remove Telraam data and use only official Berlin traffic data from daten.berlin.de

2. **sekundarschulen-berlin.de**: Contact schulverzeichnis@online.de for permission or replace entirely with bildung.berlin.de data

3. **Google Places**:
   - Remove cached POI data (names, addresses) from database
   - Store only Place IDs
   - Implement live API calls for display
   - Or switch to OpenStreetMap/Overpass API (ODbL license)

4. **Kriminalitätsatlas**: Ensure compliance with CC-BY-SA share-alike requirement if distributing derived data

5. **Attribution Page**: Create a data sources/attribution page in your application

---

*Document created: 2026-01-25*
*Last updated: 2026-01-25*
*Review recommended: Annually or when adding new data sources*

"""Berlin Open Data collector for school information"""

import httpx
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class BerlinOpenDataCollector:
    """Collector for Berlin Open Data Portal school data"""

    def __init__(self):
        # Berlin WFS endpoint for schools
        self.wfs_base_url = "https://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/s_wfs_schulen"
        self.ckan_api_url = "https://daten.berlin.de/api/3"
        self.timeout = 30.0

    async def fetch_schools_wfs(self, max_features: int = 1000) -> List[Dict[str, Any]]:
        """
        Fetch schools from Berlin WFS endpoint

        Args:
            max_features: Maximum number of features to fetch

        Returns:
            List of school dictionaries
        """
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "fis:s_wfs_schulen",
            "outputFormat": "application/json",
            "count": max_features,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"Fetching schools from WFS endpoint: {self.wfs_base_url}")
                response = await client.get(self.wfs_base_url, params=params)
                response.raise_for_status()

                data = response.json()
                features = data.get("features", [])

                logger.info(f"Successfully fetched {len(features)} schools from WFS")
                return features

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching schools: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error fetching schools from WFS: {str(e)}")
            raise

    async def fetch_schools_ckan(self, package_id: str = "schulen-wfs-ebc64e18") -> Dict[str, Any]:
        """
        Fetch school dataset metadata from CKAN API

        Args:
            package_id: CKAN package ID for the schools dataset

        Returns:
            Dataset metadata dictionary
        """
        url = f"{self.ckan_api_url}/action/package_show"
        params = {"id": package_id}

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"Fetching dataset metadata from CKAN API")
                response = await client.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                result = data.get("result", {})

                logger.info(f"Successfully fetched dataset metadata: {result.get('title', 'N/A')}")
                return result

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching CKAN metadata: {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"Error fetching from CKAN API: {str(e)}")
            raise

    def parse_school_feature(self, feature: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse WFS feature into standardized school dictionary

        Args:
            feature: GeoJSON feature from WFS response

        Returns:
            Parsed school dictionary
        """
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coordinates = geometry.get("coordinates", [None, None])

        # Extract coordinates (WFS might return [lon, lat])
        longitude = coordinates[0] if len(coordinates) > 0 else None
        latitude = coordinates[1] if len(coordinates) > 1 else None

        # Map WFS fields to our schema
        # Note: Actual field names will need to be adjusted based on real WFS response
        school_data = {
            "school_id": properties.get("id") or properties.get("schulnummer") or properties.get("objid"),
            "name": properties.get("bezeichnung") or properties.get("name") or properties.get("schulname"),
            "school_type": properties.get("schulart") or properties.get("schultyp"),
            "address": self._build_address(properties),
            "district": properties.get("bezirk") or properties.get("ortsteil"),
            "latitude": latitude,
            "longitude": longitude,
            "public_private": properties.get("schultraeger") or properties.get("traegerschaft"),
            "contact_info": {
                "phone": properties.get("telefon"),
                "email": properties.get("email"),
                "website": properties.get("www") or properties.get("homepage"),
                "street": properties.get("strasse"),
                "postal_code": properties.get("plz"),
            },
            "raw_data": properties,  # Store full properties for reference
        }

        return school_data

    def _build_address(self, properties: Dict[str, Any]) -> str:
        """Build full address from properties"""
        parts = []

        if street := properties.get("strasse"):
            parts.append(street)
        if house_number := properties.get("hausnummer"):
            parts.append(house_number)
        if postal_code := properties.get("plz"):
            if parts:
                parts.append(",")
            parts.append(postal_code)
        if city := properties.get("ort", "Berlin"):
            parts.append(city)

        return " ".join(str(p) for p in parts if p)

    async def collect_all_schools(self) -> List[Dict[str, Any]]:
        """
        Collect all schools from Berlin Open Data

        Returns:
            List of parsed school dictionaries
        """
        try:
            # Fetch raw features from WFS
            features = await self.fetch_schools_wfs()

            # Parse each feature
            schools = []
            for feature in features:
                try:
                    school = self.parse_school_feature(feature)
                    if school.get("school_id") and school.get("name"):
                        schools.append(school)
                    else:
                        logger.warning(f"Skipping school with missing ID or name: {feature.get('properties', {})}")
                except Exception as e:
                    logger.error(f"Error parsing school feature: {str(e)}")
                    continue

            logger.info(f"Successfully parsed {len(schools)} schools")
            return schools

        except Exception as e:
            logger.error(f"Error collecting schools: {str(e)}")
            raise


# Synchronous wrapper for convenience
class BerlinOpenDataCollectorSync:
    """Synchronous version of Berlin Open Data collector"""

    def __init__(self):
        self.collector = BerlinOpenDataCollector()

    def collect_all_schools(self) -> List[Dict[str, Any]]:
        """Collect all schools synchronously"""
        import asyncio
        return asyncio.run(self.collector.collect_all_schools())

    def fetch_schools_wfs(self, max_features: int = 1000) -> List[Dict[str, Any]]:
        """Fetch schools from WFS synchronously"""
        import asyncio
        return asyncio.run(self.collector.fetch_schools_wfs(max_features))

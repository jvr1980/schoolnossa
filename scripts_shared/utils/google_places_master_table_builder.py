#!/usr/bin/env python3
"""
Hybrid Master Table Builder: Google Places API + Official Data Enrichment

Uses Google Places API as primary source for clean, geocoded data,
then enriches with scraped official Berlin education data.
"""

import json
import logging
from typing import List, Dict, Optional
from pathlib import Path
import requests
from fuzzywuzzy import fuzz  # for name matching
import pandas as pd

logger = logging.getLogger(__name__)

class HybridMasterTableBuilder:
    def __init__(self, google_api_key: str, gcs_bucket_name: str):
        self.api_key = google_api_key
        self.gcs_bucket_name = gcs_bucket_name
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        
    def search_places_by_type_and_postcode(
        self, 
        place_type: str, 
        postcode: str
    ) -> List[Dict]:
        """Search Google Places for education locations"""
        
        query = f"{place_type} {postcode} Berlin Germany"
        url = f"{self.base_url}/textsearch/json"
        
        params = {
            'query': query,
            'key': self.api_key,
            'language': 'de'
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        return data.get('results', [])
    
    def get_place_details(self, place_id: str) -> Dict:
        """Get detailed information for a specific place"""
        
        url = f"{self.base_url}/details/json"
        
        params = {
            'place_id': place_id,
            'key': self.api_key,
            'fields': 'name,formatted_address,geometry,formatted_phone_number,website,rating,user_ratings_total,opening_hours,photos',
            'language': 'de'
        }
        
        response = requests.get(url, params=params)
        return response.json().get('result', {})
    
    def match_with_official_data(
        self, 
        place: Dict, 
        official_records: List[Dict]
    ) -> Optional[Dict]:
        """Match Google Place with official Berlin education data using fuzzy matching"""
        
        place_name = place.get('name', '')
        place_address = place.get('formatted_address', '')
        
        best_match = None
        best_score = 0
        
        for record in official_records:
            # Fuzzy match on name
            name_score = fuzz.ratio(
                place_name.lower(), 
                record.get('edu_location_name', '').lower()
            )
            
            # Fuzzy match on address
            address_score = fuzz.partial_ratio(
                place_address.lower(),
                record.get('edu_location_address', '').lower()
            )
            
            # Combined score (weighted towards name)
            combined_score = (name_score * 0.7) + (address_score * 0.3)
            
            if combined_score > best_score and combined_score > 75:  # 75% threshold
                best_score = combined_score
                best_match = record
        
        return best_match
    
    def build_master_table(self) -> pd.DataFrame:
        """Build master table combining Google Places + official data"""
        
        # Define search parameters
        place_types = {
            'kindergarten': 'kita',
            'preschool': 'kita', 
            'primary_school': 'school',
            'secondary_school': 'school',
            'school': 'school'
        }
        
        # Berlin postal codes (simplified - expand as needed)
        berlin_postcodes = [
            '10115', '10117', '10119', '10178', '10179',  # Mitte
            '10243', '10245', '10247', '10249',  # Friedrichshain
            # ... add all Berlin postcodes
        ]
        
        all_places = []
        
        # Search Google Places
        for postcode in berlin_postcodes:
            for google_type, our_type in place_types.items():
                places = self.search_places_by_type_and_postcode(google_type, postcode)
                
                for place in places:
                    # Get detailed info
                    details = self.get_place_details(place['place_id'])
                    
                    # Extract Google data
                    google_record = {
                        'place_id': place['place_id'],
                        'edu_location_name': details.get('name'),
                        'edu_location_type': our_type,
                        'edu_location_address': details.get('formatted_address'),
                        'edu_location_website': details.get('website'),
                        'lat': details['geometry']['location']['lat'],
                        'lon': details['geometry']['location']['lng'],
                        'phone': details.get('formatted_phone_number'),
                        'rating': details.get('rating'),
                        'user_ratings_total': details.get('user_ratings_total'),
                        'source': 'google_places'
                    }
                    
                    all_places.append(google_record)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_places)
        
        # Deduplicate by place_id
        df = df.drop_duplicates(subset=['place_id'])
        
        return df

# Example usage
if __name__ == "__main__":
    builder = HybridMasterTableBuilder(
        google_api_key="YOUR_API_KEY",
        gcs_bucket_name="schoolnossa-berlin"
    )
    
    df = builder.build_master_table()
    print(f"Found {len(df)} locations")

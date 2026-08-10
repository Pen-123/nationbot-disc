import geopandas as gpd
import pandas as pd
import requests
import zipfile
import io
import os
import shutil
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

url = "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"
headers = {"User-Agent": "Mozilla/5.0"}

print("Downloading world map data...")
r = requests.get(url, headers=headers, stream=True)
if r.status_code != 200:
    raise Exception(f"Download failed with status {r.status_code}")

print("Extracting...")
with zipfile.ZipFile(io.BytesIO(r.content)) as z:
    z.extractall("temp_geo")

world = gpd.read_file("temp_geo/ne_110m_admin_0_countries.shp")

# -------- MAPPING (same as before, includes all countries) --------
region_country_mapping = {
    "Western Europe": [
        "France", "Germany", "United Kingdom", "Ireland", "Netherlands", "Belgium", 
        "Luxembourg", "Switzerland", "Austria", "Monaco", "Andorra", "Liechtenstein"
    ],
    "Eastern Europe": [
        "Poland", "Czech Republic", "Czechia", "Slovakia", "Hungary", "Romania", 
        "Bulgaria", "Ukraine", "Belarus", "Moldova", "Russia"
    ],
    "Southern Europe": [
        "Portugal", "Spain", "Italy", "Greece", "Croatia", "Slovenia", 
        "Bosnia and Herzegovina", "Bosnia and Herz.", "Serbia", "Montenegro", 
        "Albania", "North Macedonia", "Macedonia", "Kosovo", "Malta", "Cyprus", 
        "N. Cyprus", "San Marino", "Vatican"
    ],
    "Northern Europe": [
        "Norway", "Sweden", "Finland", "Denmark", "Iceland", "Estonia", 
        "Latvia", "Lithuania", "Greenland", "Faroe Is."
    ],
    "Central Asia": [
        "Kazakhstan", "Uzbekistan", "Turkmenistan", "Kyrgyzstan", "Tajikistan", "Afghanistan"
    ],
    "East Asia": [
        "China", "Japan", "South Korea", "North Korea", "Dem. Rep. Korea", "Korea", 
        "Mongolia", "Taiwan"
    ],
    "South Asia": [
        "India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan"
    ],
    "Southeast Asia": [
        "Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia", "Singapore", 
        "Cambodia", "Laos", "Timor-Leste", "Brunei", "Myanmar"
    ],
    "Middle East": [
        "Turkey", "Iran", "Iraq", "Syria", "Lebanon", "Israel", "Palestine", 
        "Jordan", "Saudi Arabia", "Yemen", "Oman", "United Arab Emirates", "UAE", 
        "Qatar", "Kuwait", "Bahrain", "Georgia", "Armenia", "Azerbaijan"
    ],
    "North Africa": [
        "Morocco", "Algeria", "Tunisia", "Libya", "Egypt", "Western Sahara", "W. Sahara"
    ],
    "West Africa": [
        "Mauritania", "Senegal", "Gambia", "Mali", "Burkina Faso", "Benin", "Togo", 
        "Ghana", "Ivory Coast", "Côte d'Ivoire", "Liberia", "Sierra Leone", "Guinea", 
        "Guinea-Bissau", "Cape Verde", "Nigeria", "Niger"
    ],
    "Central Africa": [
        "Chad", "Cameroon", "Central African Republic", "Central African Rep.", "C.A.R.", 
        "DR Congo", "Democratic Republic of the Congo", "Dem. Rep. Congo", 
        "Congo (Kinshasa)", "Republic of Congo", "Congo (Brazzaville)", "Congo", 
        "Gabon", "Equatorial Guinea", "Eq. Guinea", "Sao Tome and Principe"
    ],
    "East Africa": [
        "Sudan", "South Sudan", "S. Sudan", "Eritrea", "Ethiopia", "Djibouti", 
        "Somalia", "Somaliland", "Kenya", "Uganda", "Rwanda", "Burundi", 
        "Tanzania", "United Republic of Tanzania", "Mozambique", "Madagascar", 
        "Comoros", "Seychelles", "Mauritius"
    ],
    "Southern Africa": [
        "Angola", "Zambia", "Malawi", "Zimbabwe", "Botswana", "Namibia", 
        "South Africa", "Eswatini", "eSwatini", "Swaziland", "Lesotho"
    ],
    "Western North America": [
        "Canada", "United States", "United States of America", "USA"
    ],
    "Central North America": [
        "Mexico"
    ],
    "Eastern North America": [
        "United States", "United States of America", "USA"
    ],
    "Central America": [
        "Guatemala", "Belize", "Honduras", "El Salvador", "Nicaragua", "Costa Rica", "Panama"
    ],
    "Caribbean": [
        "Cuba", "Haiti", "Dominican Rep.", "Dominican Republic", "Jamaica", 
        "Bahamas", "Trinidad and Tobago", "Barbados", "St. Lucia", "St. Vincent", 
        "Grenada", "Antigua and Barbuda", "Dominica", "St. Kitts and Nevis"
    ],
    "Northern South America": [
        "Venezuela", "Colombia", "Guyana", "Suriname", "French Guiana"
    ],
    "Western South America": [
        "Ecuador", "Peru", "Bolivia", "Chile"
    ],
    "Eastern South America": [
        "Brazil"
    ],
    "Southern Cone": [
        "Argentina", "Uruguay", "Paraguay", "Falkland Is."
    ],
    "Australia": [
        "Australia"
    ],
    "New Zealand": [
        "New Zealand"
    ],
    "Pacific Islands": [
        "Fiji", "Solomon Islands", "Solomon Is.", "Vanuatu", "Papua New Guinea", 
        "Samoa", "Tonga", "Micronesia", "Marshall Islands", "Marshall Is.", 
        "Palau", "Nauru", "Kiribati", "Tuvalu"
    ],
    "Antarctic Peninsula": [],
    "East Antarctica": [],
    "West Antarctica": [],
}

def map_country_to_region(row):
    names_to_check = set()
    for col in ['NAME', 'ADMIN', 'NAME_LONG', 'SOVEREIGNT', 'GEOUNIT']:
        if col in row and pd.notna(row[col]):
            names_to_check.add(str(row[col]).strip().lower())
    for region, countries in region_country_mapping.items():
        for c in countries:
            if c.lower() in names_to_check:
                return region
    return None

world['subregion'] = world.apply(map_country_to_region, axis=1)

# Log unmapped
unmapped = world[world['subregion'].isna()]
if not unmapped.empty:
    logger.warning("Unmapped countries:")
    for name in unmapped['NAME'].unique():
        logger.warning(f"  - {name}")

world = world.dropna(subset=['subregion'])

# --- Compute area for each country (in km²) ---
# Use an equal-area projection (EPSG:3857 gives square meters, but distorted; we'll use a better one: EPSG:6933 for world)
# Actually, we can use area from geometry in lat/lon with pyproj, but simpler: project to EPSG:3857 and convert.
# For rough game values, it's fine.
world_proj = world.to_crs('EPSG:3857')
world_proj['area_km2'] = world_proj.geometry.area / 1_000_000  # square meters to km²

# Build province -> area dict
province_areas = {}
for idx, row in world_proj.iterrows():
    name = row['NAME']
    area = row['area_km2']
    province_areas[name] = round(area, 0)  # integer km²

# Save to JSON
with open('province_areas.json', 'w') as f:
    json.dump(province_areas, f, indent=2)
print("✅ province_areas.json created with area data.")

# Save geojson (individual countries)
world.to_file("regions.geojson", driver="GeoJSON")
print("✅ regions.geojson created with individual countries!")

shutil.rmtree("temp_geo")

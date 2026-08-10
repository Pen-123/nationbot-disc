import geopandas as gpd
import pandas as pd
import requests
import zipfile
import io
import os
import shutil
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use the reliable S3 mirror
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

# ---------- COMPLETE MAPPING WITH ALL COUNTRIES ----------
region_country_mapping = {
    "Western Europe": ["France", "Germany", "United Kingdom", "Ireland", "Netherlands", "Belgium", "Luxembourg", "Switzerland", "Austria", "Monaco", "Andorra"],
    "Eastern Europe": ["Poland", "Czech Republic", "Czechia", "Slovakia", "Hungary", "Romania", "Bulgaria", "Ukraine", "Belarus", "Moldova", "Russia"],
    "Southern Europe": ["Portugal", "Spain", "Italy", "Greece", "Croatia", "Slovenia", "Bosnia and Herzegovina", "Serbia", "Montenegro", "Albania", "North Macedonia", "Kosovo", "Malta", "Cyprus"],
    "Northern Europe": ["Norway", "Sweden", "Finland", "Denmark", "Iceland", "Estonia", "Latvia", "Lithuania", "Greenland"],
    "Central Asia": ["Kazakhstan", "Uzbekistan", "Turkmenistan", "Kyrgyzstan", "Tajikistan", "Afghanistan"],
    "East Asia": ["China", "Japan", "South Korea", "North Korea", "Mongolia", "Taiwan"],
    "South Asia": ["India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan", "Myanmar"],
    "Southeast Asia": ["Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia", "Singapore", "Cambodia", "Laos", "Timor-Leste", "Brunei"],
    "Middle East": ["Turkey", "Iran", "Iraq", "Syria", "Lebanon", "Israel", "Palestine", "Jordan", "Saudi Arabia", "Yemen", "Oman", "United Arab Emirates", "UAE", "Qatar", "Kuwait", "Bahrain", "Georgia", "Armenia", "Azerbaijan"],
    "North Africa": ["Morocco", "Algeria", "Tunisia", "Libya", "Egypt", "Western Sahara"],
    "West Africa": ["Mauritania", "Senegal", "Gambia", "Mali", "Burkina Faso", "Benin", "Togo", "Ghana", "Ivory Coast", "Côte d'Ivoire", "Liberia", "Sierra Leone", "Guinea", "Guinea-Bissau", "Cape Verde", "Nigeria", "Niger"],
    "Central Africa": [
        "Chad", "Cameroon", "Central African Republic", "C.A.R.", 
        "DR Congo", "Democratic Republic of the Congo", "Congo (Kinshasa)", 
        "Republic of Congo", "Congo (Brazzaville)", "Congo", 
        "Gabon", "Equatorial Guinea", "Sao Tome and Principe"
    ],
    "East Africa": [
        "Sudan", "South Sudan", "Eritrea", "Ethiopia", "Djibouti", 
        "Somalia", "Kenya", "Uganda", "Rwanda", "Burundi", 
        "Tanzania", "Mozambique", "Madagascar", "Comoros", "Seychelles"
    ],
    "Southern Africa": ["Angola", "Zambia", "Malawi", "Zimbabwe", "Botswana", "Namibia", "South Africa", "Eswatini", "Lesotho"],
    "Western North America": ["Canada", "United States", "United States of America", "USA"],
    "Central North America": ["Mexico"],
    "Eastern North America": ["United States", "United States of America", "USA"],
    "Mexico": ["Mexico"],
    "Central America": ["Guatemala", "Belize", "Honduras", "El Salvador", "Nicaragua", "Costa Rica", "Panama"],
    "Northern South America": ["Venezuela", "Colombia", "Guyana", "Suriname", "French Guiana"],
    "Western South America": ["Ecuador", "Peru", "Bolivia", "Chile"],
    "Eastern South America": ["Brazil"],
    "Brazil": ["Brazil"],
    "Southern Cone": ["Argentina", "Uruguay", "Paraguay"],
    "Australia": ["Australia"],
    "New Zealand": ["New Zealand"],
    "Pacific Islands": ["Fiji", "Solomon Islands", "Vanuatu", "Papua New Guinea", "Samoa", "Tonga", "Micronesia", "Marshall Islands", "Palau", "Nauru", "Kiribati", "Tuvalu"],
    "Antarctic Peninsula": [],
    "East Antarctica": [],
    "West Antarctica": [],
}

def map_country_to_region(country):
    if not country:
        return None
    country_lower = country.lower()
    for region, countries in region_country_mapping.items():
        for c in countries:
            if c.lower() == country_lower:
                return region
    return None

world['subregion'] = world['NAME'].apply(map_country_to_region)

# Log unmapped countries
unmapped = world[world['subregion'].isna()]
if not unmapped.empty:
    logger.warning(f"⚠️ The following countries were not mapped to any region:")
    for name in unmapped['NAME'].unique():
        logger.warning(f"  - {name}")
    logger.warning("Please add them to region_country_mapping in generate_geojson.py and re-run.")
else:
    logger.info("✅ All countries mapped successfully.")

# Remove countries that don't have a region (so the game's region logic doesn't break)
world = world.dropna(subset=['subregion'])

# --- CHANGED: Save individual countries, NOT dissolved regions ---
world.to_file("regions.geojson", driver="GeoJSON")
print("✅ regions.geojson created with individual countries!")

shutil.rmtree("temp_geo")

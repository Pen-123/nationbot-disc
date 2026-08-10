import geopandas as gpd
import pandas as pd
import requests
import zipfile
import io
import os

# Download Natural Earth countries
url = "https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_0_countries.zip"
r = requests.get(url)
with zipfile.ZipFile(io.BytesIO(r.content)) as z:
    z.extractall("temp_geo")

world = gpd.read_file("temp_geo/ne_110m_admin_0_countries.shp")

# ---------- MAP YOUR SUB‑REGIONS TO COUNTRY NAMES ----------
# This is the mapping you need to adjust to your own region names.
# I've made a reasonable guess – you can edit this dictionary.
region_country_mapping = {
    "Western Europe": ["France", "Germany", "United Kingdom", "Ireland", "Netherlands", "Belgium", "Luxembourg", "Switzerland", "Austria"],
    "Eastern Europe": ["Poland", "Czech Republic", "Slovakia", "Hungary", "Romania", "Bulgaria", "Ukraine", "Belarus", "Moldova", "Russia"],
    "Southern Europe": ["Portugal", "Spain", "Italy", "Greece", "Croatia", "Slovenia", "Bosnia and Herzegovina", "Serbia", "Montenegro", "Albania", "North Macedonia", "Kosovo"],
    "Northern Europe": ["Norway", "Sweden", "Finland", "Denmark", "Iceland", "Estonia", "Latvia", "Lithuania"],
    "Central Asia": ["Kazakhstan", "Uzbekistan", "Turkmenistan", "Kyrgyzstan", "Tajikistan", "Afghanistan"],
    "East Asia": ["China", "Japan", "South Korea", "North Korea", "Mongolia", "Taiwan"],
    "South Asia": ["India", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal", "Bhutan", "Myanmar"],
    "Southeast Asia": ["Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia", "Singapore", "Cambodia", "Laos"],
    "Middle East": ["Turkey", "Iran", "Iraq", "Syria", "Lebanon", "Israel", "Palestine", "Jordan", "Saudi Arabia", "Yemen", "Oman", "UAE", "Qatar", "Kuwait", "Bahrain", "Georgia", "Armenia", "Azerbaijan"],
    "North Africa": ["Morocco", "Algeria", "Tunisia", "Libya", "Egypt", "Western Sahara"],
    "West Africa": ["Mauritania", "Senegal", "Gambia", "Mali", "Burkina Faso", "Benin", "Togo", "Ghana", "Ivory Coast", "Liberia", "Sierra Leone", "Guinea", "Guinea-Bissau", "Cape Verde"],
    "Central Africa": ["Niger", "Nigeria", "Chad", "Cameroon", "Central African Republic", "DR Congo", "Republic of Congo", "Gabon", "Equatorial Guinea"],
    "East Africa": ["Sudan", "South Sudan", "Eritrea", "Ethiopia", "Djibouti", "Somalia", "Kenya", "Uganda", "Rwanda", "Burundi", "Tanzania", "Mozambique", "Madagascar", "Comoros", "Seychelles"],
    "Southern Africa": ["Angola", "Zambia", "Malawi", "Zimbabwe", "Botswana", "Namibia", "South Africa", "Eswatini", "Lesotho"],
    "Western North America": ["Canada", "United States"],
    "Central North America": ["Mexico"],
    "Eastern North America": ["United States"],   # <-- comma fixed
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

# Create a column 'subregion' with the region name for each country
def map_country_to_region(country):
    for region, countries in region_country_mapping.items():
        if country in countries:
            return region
    return None

world['subregion'] = world['NAME'].apply(map_country_to_region)

# Drop countries not in our list
world = world.dropna(subset=['subregion'])

# Dissolve (merge) polygons by subregion
regions = world.dissolve(by='subregion', aggfunc='sum')
regions = regions.reset_index()  # make 'subregion' a column again

# Save to GeoJSON
regions.to_file("regions.geojson", driver="GeoJSON")
print("✅ regions.geojson created!")

# Cleanup
import shutil
shutil.rmtree("temp_geo")

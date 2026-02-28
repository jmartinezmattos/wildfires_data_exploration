    import ee
    from datetime import datetime, timedelta

    ee.Initialize(project='fire-detection-uruguay')

# Uruguay center region
lat, lon = -32.8, -56.2
geometry = ee.Geometry.Point([lon, lat]).buffer(50000)  # 50km buffer

start_date = (datetime.now() - timedelta(days=30)).isoformat()
end_date = datetime.now().isoformat()

# Sentinel-2 data
s2 = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
    .filterBounds(geometry) \
    .filterDate(start_date, end_date) \
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))

s2_count = s2.size().getInfo()
print(f"Sentinel-2 images found: {s2_count}")


    
    s2_composite = s2.median()
    s2_bands = s2_composite.select(['B1', 'B2', 'B3', 'B4', 'B8', 'B11', 'B12'])
    print(f"Sentinel-2 bands selected: B1, B2, B3, B4, B8, B11, B12")

# Sentinel-5P TROPOMI data
s5p = ee.ImageCollection('COPERNICUS/S5P/TROPOMI') \
    .filterBounds(geometry) \
    .filterDate(start_date, end_date)

s5p_count = s5p.size().getInfo()
print(f"\nSentinel-5P TROPOMI observations found: {s5p_count}")

if s5p_count > 0:
    s5p_images = s5p.toList(min(s5p_count, 5)).getInfo()
    for i, img in enumerate(s5p_images):
        date = datetime.fromtimestamp(img['properties']['system:time_start']/1000)
        bands = [b['id'] for b in img['bands'][:3]]
        print(f"  {i+1}. {date} - Bands: {bands}")
    
    s5p_composite = s5p.median()
    available_bands = s5p.first().bandNames().getInfo()
    print(f"Available TROPOMI bands: {available_bands}")

print("\n✓ Data retrieved successfully. You can now export using ee.batch.Export")

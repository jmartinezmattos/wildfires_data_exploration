
import requests

BUFFER_METERS = 2000
THUMB_SIZE = 1024

def download_thumbnail(image, filename, point, satellite, bands=['B4','B3','B2'], size=THUMB_SIZE):

    region = point.buffer(BUFFER_METERS).bounds().getInfo()['coordinates'][0]

    if satellite == "landsat-8":
        bands = ['SR_B4', 'SR_B3', 'SR_B2']
        image = image.select(bands).multiply(0.0000275).add(-0.2)
        vmin, vmax = 0, 0.3
    elif satellite == "sentinel-2":
        # Earth Engine thumbnail visualization supports 1-3 bands only.
        bands = ['B4', 'B3', 'B2']
        vmin, vmax = 0, 6000
    elif satellite == "aqua":
        bands = ['sur_refl_b01','sur_refl_b04','sur_refl_b03']
        vmin, vmax = 0, 5000
    elif satellite == "fengyun":
        bands = ['Channel0001','Channel0002','Channel0003']
        vmin, vmax = 0, 4000
    else:
        raise ValueError(f"Satellite not supported: {satellite}")

    try:
        thumb_url = image.getThumbURL({
            'dimensions': size, # pixels
            'region': region, # geografic region
            'bands': bands,
            'min': vmin,
            'max': vmax,
        })
        r = requests.get(thumb_url)
        if r.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(r.content)
            return True
        else:
            print(f"Error HTTP {r.status_code} downloading {filename}")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
    return False
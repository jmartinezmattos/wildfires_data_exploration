example_config = {
    "GEE_PROJECT": "cellular-retina-276416",
    "IMAGES_SATELLITE": "sentinel-2",
    # sentinel-2, landsat-8, aqua, fengshun
    "COUNTRY": "Uruguay",
    "FIRMS_INSTRUMENT": "VIIRS S-NPP",
    # MODIS, VIIRS S-NPP, VIIRS NOAA-20"
    "CSV_PATH": None,
    "BUFFER_METERS": {
        "default": 2000,
        "aqua": 30000,
        "fengyun": 30000
    },
    "THUMB_SIZE": 1024,
    "MAX_IMAGES_PER_POINT": 1,
    "MAX_TIME_DIFF_HOURS": 10,
    "CLOUD_FILTER_PERCENTAGE": 85,
    "OLD_RUN_DIR": None,
    "NUM_THREADS": 10,
    "MIN_CONFIDENCE_MODIS": 10,
    # Enteros del 1 al 100
    "MIN_CONFIDENCE_VIIRS": "l"
    # "l", "n", "h"
}
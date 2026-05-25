import os
from datetime import datetime

# Cloudflare R2 Configuration
CF_R2_ACCESS_KEY_ID = os.getenv('CF_R2_ACCESS_KEY_ID', '')
CF_R2_SECRET_ACCESS_KEY = os.getenv('CF_R2_SECRET_ACCESS_KEY', '')
CF_R2_BUCKET_NAME = os.getenv('CF_R2_BUCKET_NAME', '')
CF_R2_ENDPOINT_URL = os.getenv('CF_R2_ENDPOINT_URL', '')

# Date-based partitioning
CURRENT_DATE = datetime.now()
YEAR = CURRENT_DATE.strftime('%Y')
MONTH = CURRENT_DATE.strftime('%m')
DAY = CURRENT_DATE.strftime('%d')

# R2 Paths (same partition structure as S3)
R2_BASE_PATH = 'boutiqaat-data'
R2_IMAGES_PATH = f'{R2_BASE_PATH}/year={YEAR}/month={MONTH}/day={DAY}/women-makeup/images'
R2_EXCEL_PATH = f'{R2_BASE_PATH}/year={YEAR}/month={MONTH}/day={DAY}/women-makeup'

# Website Configuration
BASE_URL = 'https://www.boutiqaat.com'
CATEGORY_URL = f'{BASE_URL}/ar-kw/women/makeup/c/'
MAIN_CATEGORY = 'makeup'

# Timeout and retry settings
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# Image settings
IMAGE_QUALITY = 80
MAX_IMAGE_SIZE = (400, 400)

# Local temporary directory
TEMP_DIR = './temp_downloads'

# Excel settings
EXCEL_DATE_STR = CURRENT_DATE.strftime('%Y-%m-%d')

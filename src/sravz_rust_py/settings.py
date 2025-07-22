import os

# settings.py
LOG_FILE = None
LOG_LEVEL = 'DEBUG'
MAX_SIMILARITY_SEARCH_COUNT = 5
CONTABO_BUCKET = 'sravz'
CONTABO_BUCKET_PREFIX = 'sravz-dev' if os.environ.get(
    'NODE_ENV') == 'vagrant' else 'sravz-production'
CONTABO_URL = os.environ.get('CONTABO_URL')
CONTABO_BASE_URL = os.environ.get('CONTABO_BASE_URL')
CONTABO_KEY = os.environ.get('CONTABO_KEY')
CONTABO_SECRET = os.environ.get('CONTABO_SECRET')
IDRIVEE2_BASE_URL = os.environ.get('IDRIVEE2_URL')
IDRIVEE2_KEY = os.environ.get('IDRIVEE2_KEY')
IDRIVEE2_SECRET = os.environ.get('IDRIVEE2_SECRET')
CONTABO_DATA_BUCKET = 'sravz-data'
MUTUAL_FUNDS_FUNDAMENTAL_DATA_PREFIX = 'mutual_funds/fundamentals/'

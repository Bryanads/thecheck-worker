import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente do .env
load_dotenv()

# Credenciais do banco de dados
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Chaves de API
API_KEY_STORMGLASS = os.getenv('API_KEY_1')

# Configurações globais para as requisições
OUTPUT_DIR = 'data' # Diretório onde os JSONs temporários serão salvos
REQUEST_DIR = os.path.join(OUTPUT_DIR, 'requests') # Diretório para requisições
TREATED_DIR = os.path.join(OUTPUT_DIR, 'treated') # Diretório para dados tratados
FORECAST_DAYS = 10 # Quantidade de dias de previsão
HOURS_FILTER = list(range(5, 18)) # 5 AM to 5 PM (local time)

# StormGlass.io API endpoint URLs
WEATHER_API_URL = "https://api.stormglass.io/v2/weather/point"
TIDE_SEA_LEVEL_API_URL = "https://api.stormglass.io/v2/tide/sea-level/point"
TIDE_EXTREMES_API_URL = "https://api.stormglass.io/v2/tide/extremes/point"

# Parâmetros para /weather/point endpoint
PARAMS_WEATHER_API = [
    'waveHeight', 'waveDirection', 'wavePeriod', 'swellHeight', 'swellDirection',
    'swellPeriod', 'secondarySwellHeight', 'secondarySwellDirection',
    'secondarySwellPeriod', 'windSpeed', 'windDirection', 'waterTemperature',
    'airTemperature', 'currentSpeed', 'currentDirection'
]
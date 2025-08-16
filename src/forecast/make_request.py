import arrow
import asyncio
import requests
import json
import os
import sys
import decimal
from src.db.connection import init_async_db_pool
from src.db.queries import get_all_spots
from src.utils.config import (
    API_KEY_STORMGLASS, REQUEST_DIR, FORECAST_DAYS,
    WEATHER_API_URL, TIDE_SEA_LEVEL_API_URL, TIDE_EXTREMES_API_URL, PARAMS_WEATHER_API
)

def choose_spot_from_db(available_spots):
    if not available_spots:
        return None, None

    print("\nEscolha um dos spots disponíveis no banco de dados:")
    for i, spot in enumerate(available_spots):
        print(f"{i + 1}. {spot['spot_name']}")

    while True:
        try:
            choice = int(input("Digite o número do spot: ")) - 1
            if 0 <= choice < len(available_spots):
                return available_spots[choice]
            else:
                print("Escolha inválida. Tente novamente.")
        except ValueError:
            print("Entrada inválida. Por favor, digite um número.")

def fetch_and_save_data(api_url, params, headers, filename, label):
    print(f"Buscando dados de {label}...")
    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        os.makedirs(REQUEST_DIR, exist_ok=True)
        with open(os.path.join(REQUEST_DIR, filename), 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Dados de {label} salvos em {filename}")
        return data
    except Exception as e:
        print(f"Erro ao buscar dados de {label}: {e}")
        return None

async def main():
    """
    Função principal assíncrona para buscar spots do banco e, em seguida,
    buscar os dados de previsão da API externa.
    """
    # Inicializa o pool de conexões assíncronas
    await init_async_db_pool()

    # Busca os spots de forma assíncrona
    available_spots = await get_all_spots()
    if not available_spots:
        sys.exit(1)

    selected_spot = choose_spot_from_db(available_spots)
    if selected_spot is None:
        sys.exit(0)

    def decimal_default(obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        raise TypeError

    os.makedirs(REQUEST_DIR, exist_ok=True)
    with open(os.path.join(REQUEST_DIR, 'current_spot.json'), 'w') as f:
        json.dump(selected_spot, f, ensure_ascii=False, indent=4, default=decimal_default)

    start = arrow.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start.shift(days=FORECAST_DAYS).replace(hour=23, minute=59, second=59, microsecond=999999)
    headers = {'Authorization': API_KEY_STORMGLASS}

    fetch_and_save_data(
        WEATHER_API_URL,
        {'lat': selected_spot['latitude'], 'lng': selected_spot['longitude'], 'params': ','.join(PARAMS_WEATHER_API), 'start': int(start.timestamp()), 'end': int(end.timestamp())},
        headers, 'weather_data.json', "clima"
    )

    fetch_and_save_data(
        TIDE_SEA_LEVEL_API_URL,
        {'lat': selected_spot['latitude'], 'lng': selected_spot['longitude'], 'params': 'seaLevel', 'start': int(start.timestamp()), 'end': int(end.timestamp())},
        headers, 'sea_level_data.json', "nível do mar"
    )

    fetch_and_save_data(
        TIDE_EXTREMES_API_URL,
        {'lat': selected_spot['latitude'], 'lng': selected_spot['longitude'], 'start': int(start.timestamp()), 'end': int(end.timestamp())},
        headers, 'tide_extremes_data.json', "extremos da maré"
    )

if __name__ == "__main__":
    # Executa a função principal assíncrona
    asyncio.run(main())
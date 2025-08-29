import arrow
import asyncio
import json
import os
import sys
import decimal
from src.db.queries import get_all_spots, insert_forecast_data, delete_old_forecast_data
from src.db.connection import init_async_db_pool
from src.utils.config import (
    API_KEY_STORMGLASS, REQUEST_DIR, FORECAST_DAYS,
    WEATHER_API_URL, TIDE_SEA_LEVEL_API_URL, TIDE_EXTREMES_API_URL, PARAMS_WEATHER_API,
    TREATED_DIR
)
from src.forecast.data_processing import merge_stormglass_data, filter_forecast_time
from src.utils.utils import convert_to_localtime, load_json_data
from src.forecast.make_request import fetch_and_save_data

def decimal_default(obj):
    """Função auxiliar para serializar objetos Decimal em JSON."""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

async def process_spot(spot_details):
    """
    Executa todo o processo de busca, tratamento e inserção de dados para um único spot.
    """
    spot_id = spot_details['spot_id']
    spot_name = spot_details['spot_name']
    latitude = spot_details['latitude']
    longitude = spot_details['longitude']

    print(f"\n{'='*50}")
    print(f"INICIANDO PROCESSAMENTO PARA: {spot_name} (ID: {spot_id})")
    print(f"{'='*50}")

    # Configuração do período de previsão
    start = arrow.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start.shift(days=FORECAST_DAYS).replace(hour=23, minute=59, second=59, microsecond=999999)
    headers = {'Authorization': API_KEY_STORMGLASS}

    # Busca os dados das APIs
    print(f"Buscando dados de tempo para o spot {spot_name}...")
    fetch_and_save_data(
        WEATHER_API_URL,
        {'lat': latitude, 'lng': longitude, 'params': ','.join(PARAMS_WEATHER_API), 'start': int(start.timestamp()), 'end': int(end.timestamp())},
        headers, 'weather_data.json', "weather"
    )
    print(f"Buscando dados de nível do mar para o spot {spot_name}...")
    fetch_and_save_data(
        TIDE_SEA_LEVEL_API_URL,
        {'lat': latitude, 'lng': longitude, 'params': 'seaLevel', 'start': int(start.timestamp()), 'end': int(end.timestamp())},
        headers, 'sea_level_data.json', "sea level"
    )
    print(f"Buscando dados de marés extremas para o spot {spot_name}...")
    fetch_and_save_data(
        TIDE_EXTREMES_API_URL,
        {'lat': latitude, 'lng': longitude, 'start': int(start.timestamp()), 'end': int(end.timestamp())},
        headers, 'tide_extremes_data.json', "tide extremes"
    )

    # Etapa 1: Merge dos dados
    print("Mesclando dados de previsão...")
    merged = merge_stormglass_data('weather_data.json', 'sea_level_data.json', 'forecast_data.json')
    if not merged:
        print(f"ERRO: Falha ao mesclar dados para o spot {spot_name}. Pulando para o próximo.")
        return

    # Etapa 2: Converter e filtrar
    print("Convertendo para horário local e filtrando...")
    localtime_data = convert_to_localtime(merged)
    filtered = filter_forecast_time(localtime_data)

    if not filtered:
        print(f"AVISO: Nenhum dado de previsão válido após o filtro para o spot {spot_name}. Pulando inserção.")
    else:
        os.makedirs(TREATED_DIR, exist_ok=True)
        with open(os.path.join(TREATED_DIR, f'forecast_data_{spot_id}.json'), 'w', encoding='utf-8') as f:
            json.dump(filtered, f, ensure_ascii=False, indent=4)
        
        # Inserir no banco de dados
        await insert_forecast_data(spot_id, filtered)
    
    print(f"\n--- SUCESSO: Dados para {spot_name} (ID: {spot_id}) processados e inseridos. ---")

async def main():
    """
    Função principal que busca e insere dados de previsão para uma lista de spots
    fornecida via linha de comando.
    """
    # Verifica se os IDs dos spots foram passados como argumento
    if len(sys.argv) < 2:
        print("ERRO: Nenhum ID de spot fornecido.")
        print("Uso: python fetch_and_insert_all.py <spot_id_1> <spot_id_2> ...")
        sys.exit(1)
    
    spot_ids_from_args = []
    for arg in sys.argv[1:]:
        try:
            spot_ids_from_args.append(int(arg))
        except ValueError:
            print(f"AVISO: Argumento '{arg}' não é um ID numérico válido e será ignorado.")

    # Inicializa o pool de conexões com o banco de dados
    await init_async_db_pool()

    # Busca todos os spots disponíveis no banco para validação
    all_spots = await get_all_spots()
    if not all_spots:
        print("ERRO: Nenhum spot encontrado no banco de dados. Abortando.")
        sys.exit(1)

    # Cria um mapa de spot_id para os detalhes do spot para facilitar a busca
    spots_map = {spot['spot_id']: spot for spot in all_spots}

    # Itera sobre os IDs fornecidos e processa cada um
    for spot_id in spot_ids_from_args:
        spot_to_process = spots_map.get(spot_id)
        if spot_to_process:
            try:
                await process_spot(spot_to_process)
            except Exception as e:
                print(f"\nERRO INESPERADO ao processar o spot ID {spot_id}: {e}")
                print("Continuando para o próximo spot...")
        else:
            print(f"AVISO: Spot com ID {spot_id} não encontrado no banco de dados. Pulando.")
    
    print("\nProcesso finalizado para todos os spots solicitados.")

    await delete_old_forecast_data(7)

if __name__ == "__main__":
    asyncio.run(main())
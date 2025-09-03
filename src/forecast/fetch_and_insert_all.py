import arrow
import asyncio
import json
import os
import sys
from src.db.queries import get_all_spots, insert_forecast_data, delete_old_forecast_data
from src.db.connection import init_async_db_pool
from src.utils.config import (
    API_KEY_STORMGLASS, FORECAST_DAYS,
    WEATHER_API_URL, TIDE_SEA_LEVEL_API_URL, PARAMS_WEATHER_API,
    TREATED_DIR
)
from src.forecast.data_processing import merge_stormglass_data
from src.forecast.make_request import fetch_and_save_data

async def process_spot(spot_details):
    """
    Executa o processo de busca, tratamento e inserção de dados para um único spot.
    """
    spot_id = spot_details['spot_id']
    spot_name = spot_details['spot_name']
    latitude = spot_details['latitude']
    longitude = spot_details['longitude']

    print(f"\n{'='*50}\nINICIANDO PROCESSAMENTO PARA: {spot_name} (ID: {spot_id})\n{'='*50}")

    start = arrow.now().replace(hour=0, minute=0, second=0)
    end = start.shift(days=FORECAST_DAYS)
    headers = {'Authorization': API_KEY_STORMGLASS}
    
    # Busca os dados de tempo e nível do mar (CORREÇÃO: 'await' removido)
    fetch_and_save_data(
        WEATHER_API_URL,
        {'lat': latitude, 'lng': longitude, 'params': ','.join(PARAMS_WEATHER_API), 'start': start.timestamp(), 'end': end.timestamp()},
        headers, 'weather_data.json', f"Tempo para {spot_name}"
    )
    fetch_and_save_data(
        TIDE_SEA_LEVEL_API_URL,
        {'lat': latitude, 'lng': longitude, 'start': start.timestamp(), 'end': end.timestamp()},
        headers, 'sea_level_data.json', f"Nível do mar para {spot_name}"
    )

    # Etapa 1: Merge dos dados (já calcula o tide_type)
    print("Mesclando dados de previsão...")
    merged = merge_stormglass_data('weather_data.json', 'sea_level_data.json', f'treated_forecast_{spot_id}.json')
    if not merged:
        print(f"ERRO: Falha ao mesclar dados para o spot {spot_name}. Pulando para o próximo.")
        return

    # Inserir no banco de dados
    await insert_forecast_data(spot_id, merged)
    
    print(f"\n--- SUCESSO: Dados para {spot_name} (ID: {spot_id}) processados e inseridos. ---")

async def main():
    """
    Função principal que busca e insere dados de previsão para uma lista de spots
    fornecida via linha de comando.
    """
    if len(sys.argv) < 2:
        print("ERRO: Nenhum ID de spot fornecido.\nUso: python -m src.forecast.fetch_and_insert_all <spot_id_1> <spot_id_2> ...")
        sys.exit(1)
    
    spot_ids_from_args = [int(arg) for arg in sys.argv[1:] if arg.isdigit()]
    
    await init_async_db_pool()
    all_spots = await get_all_spots()
    if not all_spots:
        print("ERRO: Nenhum spot encontrado no banco de dados. Abortando.")
        sys.exit(1)

    spots_map = {spot['spot_id']: spot for spot in all_spots}
    tasks = []
    for spot_id in spot_ids_from_args:
        if spot_id in spots_map:
            tasks.append(process_spot(spots_map[spot_id]))
        else:
            print(f"AVISO: Spot com ID {spot_id} não encontrado no banco de dados. Pulando.")
    
    # Processa os spots em paralelo
    await asyncio.gather(*tasks)
    
    print("\nProcesso finalizado para todos os spots solicitados.")
    await delete_old_forecast_data(7)

if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import json
import os
import sys
from src.db.connection import init_async_db_pool
from src.db.queries import insert_forecast_data, insert_extreme_tides_data
from src.utils.config import REQUEST_DIR, TREATED_DIR
from src.forecast.data_processing import merge_stormglass_data, filter_forecast_time
from src.utils.utils import convert_to_localtime, load_json_data

def load_selected_spot():
    """Carrega os dados do spot selecionado do arquivo JSON."""
    return load_json_data('current_spot.json', REQUEST_DIR)

async def main():
    """
    Função principal assíncrona para processar dados de previsão já buscados
    e inseri-los no banco de dados.
    """
    # Inicializa o pool de conexões assíncronas
    await init_async_db_pool()

    spot = load_selected_spot()
    if not spot:
        print("Nenhum spot selecionado. Rode make_request.py primeiro.")
        sys.exit(1)

    spot_id = spot['spot_id']

    # Etapa 1: Merge dos dados de clima e nível do mar
    merged = merge_stormglass_data('weather_data.json', 'sea_level_data.json', 'forecast_data.json')
    if not merged:
        print("Erro ao mesclar dados. Abortando inserção.")
        sys.exit(1)

    # Etapa 2: Converter para horário local e filtrar
    localtime_data = convert_to_localtime(merged)
    filtered = filter_forecast_time(localtime_data)

    if not filtered:
        print("Nenhum dado de previsão válido após filtro. Abortando.")
        sys.exit(1)

    os.makedirs(TREATED_DIR, exist_ok=True)
    with open(os.path.join(TREATED_DIR, 'forecast_data.json'), 'w', encoding='utf-8') as f:
        json.dump(filtered, f, ensure_ascii=False, indent=4)

    # Inserir os dados de previsão no banco de forma assíncrona
    await insert_forecast_data(spot_id, filtered)

    # Etapa 3: Processar e inserir dados de marés extremas
    tide_raw = load_json_data('tide_extremes_data.json', REQUEST_DIR)
    if tide_raw and 'data' in tide_raw:
        tide_data = convert_to_localtime(tide_raw['data'])
        with open(os.path.join(TREATED_DIR, 'tide_extremes_filtered.json'), 'w', encoding='utf-8') as f:
            json.dump(tide_data, f, ensure_ascii=False, indent=4)

        # Inserir os dados de maré no banco de forma assíncrona
        await insert_extreme_tides_data(spot_id, tide_data)
    else:
        print("Erro ao carregar dados de marés extremas. Abortando.")
        sys.exit(1)
    
    print("Processo de salvamento e inserção de dados concluído com sucesso.")

if __name__ == "__main__":
    # Executa a função principal assíncrona
    asyncio.run(main())
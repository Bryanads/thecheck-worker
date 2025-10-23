# bryanads/thecheck-worker/thecheck-worker-5d2c32562db70fa8dd5be23d1229053a0125233a/src/forecast/data_processing.py
from src.utils.config import REQUEST_DIR, TREATED_DIR # Mantido caso precise no futuro
from src.utils.utils import load_json_data, save_json_data, determine_tide_phase

def filter_forecast_time(data):
    """Filtra os dados para manter apenas as horas de interesse (ex: 5h às 17h)."""
    # Esta função pode ser ajustada ou removida se você quiser guardar dados de 24h
    filtered = []
    for entry in data:
        try:
            # Supondo que 'time' já está no formato ISO com fuso horário
            hour = int(entry['time'][11:13])
            # REMOVIDO: O filtro de horário não deve ser aplicado aqui,
            # pois o worker precisa dos dados completos para calcular scores
            # if 5 <= hour <= 17:
            #     filtered.append(entry)
            filtered.append(entry) # Mantém todas as horas
        except Exception as e:
            print(f"Erro ao processar horário para filtro (ignorando filtro): {entry.get('time')} | {e}")
            filtered.append(entry) # Adiciona mesmo se houver erro na extração da hora
    return filtered

# --- FUNÇÃO CORRIGIDA ---
def merge_stormglass_data(weather_data: dict, sea_level_data: dict, output_filename: str = None):
    """
    Mescla os dados de tempo e nível do mar (recebidos como dicionários),
    e calcula o tipo de maré. Opcionalmente salva em arquivo.
    """
    # REMOVIDO: Não carrega mais de arquivos
    # weather_data = load_json_data(weather_filename, REQUEST_DIR)
    # sea_level_data = load_json_data(sea_level_filename, REQUEST_DIR)

    if not weather_data or 'hours' not in weather_data or not sea_level_data or 'data' not in sea_level_data:
        print("Dados de tempo ou nível do mar inválidos para o merge.")
        return None

    # 1. Adiciona o tipo de maré aos dados de nível do mar usando sua função utilitária
    # Garante que 'data' exista e seja uma lista antes de passar para determine_tide_phase
    sea_level_list = sea_level_data.get('data', [])
    if not isinstance(sea_level_list, list):
         print(f"ERRO: 'data' em sea_level_data não é uma lista. Tipo: {type(sea_level_list)}")
         return None
    sea_level_with_tide_type = determine_tide_phase(sea_level_list)


    # 2. Cria dicionários para um merge eficiente
    # Garante que 'hours' exista e seja uma lista
    weather_hours_list = weather_data.get('hours', [])
    if not isinstance(weather_hours_list, list):
         print(f"ERRO: 'hours' em weather_data não é uma lista. Tipo: {type(weather_hours_list)}")
         return None
    weather_by_time = {entry['time']: entry for entry in weather_hours_list if 'time' in entry}
    sea_level_by_time = {entry['time']: entry for entry in sea_level_with_tide_type if 'time' in entry}


    merged = []
    # Itera sobre as horas dos dados de tempo (geralmente mais completos)
    for time_str, weather in weather_by_time.items():
        sea_level_entry = sea_level_by_time.get(time_str, {}) # Pega o correspondente ou um dict vazio

        # Função auxiliar para obter valor aninhado com segurança
        def get_nested(data, keys, default=None):
            val = data
            try:
                for key in keys:
                    val = val[key]
                return val
            except (KeyError, TypeError):
                return default

        merged.append({
            'time': time_str,
            'waveHeight_sg': get_nested(weather, ['waveHeight', 'sg']),
            'waveDirection_sg': get_nested(weather, ['waveDirection', 'sg']),
            'wavePeriod_sg': get_nested(weather, ['wavePeriod', 'sg']),
            'swellHeight_sg': get_nested(weather, ['swellHeight', 'sg']),
            'swellDirection_sg': get_nested(weather, ['swellDirection', 'sg']),
            'swellPeriod_sg': get_nested(weather, ['swellPeriod', 'sg']),
            'secondarySwellHeight_sg': get_nested(weather, ['secondarySwellHeight', 'sg']),
            'secondarySwellDirection_sg': get_nested(weather, ['secondarySwellDirection', 'sg']),
            'secondarySwellPeriod_sg': get_nested(weather, ['secondarySwellPeriod', 'sg']),
            'windSpeed_sg': get_nested(weather, ['windSpeed', 'sg']),
            'windDirection_sg': get_nested(weather, ['windDirection', 'sg']),
            'waterTemperature_sg': get_nested(weather, ['waterTemperature', 'sg']),
            'airTemperature_sg': get_nested(weather, ['airTemperature', 'sg']),
            'currentSpeed_sg': get_nested(weather, ['currentSpeed', 'sg']),
            'currentDirection_sg': get_nested(weather, ['currentDirection', 'sg']),
            'seaLevel_sg': sea_level_entry.get('sg'), # Já está no nível superior
            'tide_type': sea_level_entry.get('tide_type') # Campo calculado
        })

    # Ordena pelo timestamp antes de retornar
    merged.sort(key=lambda x: x['time'])

    # Salva em arquivo apenas se output_filename for fornecido
    if output_filename:
        try:
            save_json_data(merged, output_filename, TREATED_DIR)
            print(f"Dados mesclados e salvos em: {output_filename}")
        except Exception as e:
             print(f"Erro ao salvar dados mesclados em {output_filename}: {e}")
             # Continua mesmo se salvar falhar, pois o worker precisa dos dados merged

    return merged # Retorna a lista de dicionários mesclados
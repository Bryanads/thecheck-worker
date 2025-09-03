from src.utils.config import REQUEST_DIR, TREATED_DIR
from src.utils.utils import load_json_data, save_json_data, determine_tide_phase

def filter_forecast_time(data):
    """Filtra os dados para manter apenas as horas de interesse (ex: 5h às 17h)."""
    # Esta função pode ser ajustada ou removida se você quiser guardar dados de 24h
    filtered = []
    for entry in data:
        try:
            # Supondo que 'time' já está no formato ISO com fuso horário
            hour = int(entry['time'][11:13]) 
            if 5 <= hour <= 17:
                filtered.append(entry)
        except Exception as e:
            print(f"Erro ao filtrar horário: {entry.get('time')} | {e}")
    return filtered

def merge_stormglass_data(weather_filename, sea_level_filename, output_filename):
    """
    Mescla os dados de tempo e nível do mar, e calcula o tipo de maré.
    """
    weather_data = load_json_data(weather_filename, REQUEST_DIR)
    sea_level_data = load_json_data(sea_level_filename, REQUEST_DIR)

    if not weather_data or 'hours' not in weather_data or not sea_level_data or 'data' not in sea_level_data:
        print("Dados de tempo ou nível do mar inválidos para o merge.")
        return None

    # 1. Adiciona o tipo de maré aos dados de nível do mar usando sua função utilitária
    sea_level_with_tide_type = determine_tide_phase(sea_level_data['data'])

    # 2. Cria dicionários para um merge eficiente
    weather_by_time = {entry['time']: entry for entry in weather_data['hours']}
    sea_level_by_time = {entry['time']: entry for entry in sea_level_with_tide_type}

    merged = []
    for time_str, weather in weather_by_time.items():
        sea_level_entry = sea_level_by_time.get(time_str, {})
        merged.append({
            'time': time_str,
            'waveHeight_sg': weather.get('waveHeight', {}).get('sg'),
            'waveDirection_sg': weather.get('waveDirection', {}).get('sg'),
            'wavePeriod_sg': weather.get('wavePeriod', {}).get('sg'),
            'swellHeight_sg': weather.get('swellHeight', {}).get('sg'),
            'swellDirection_sg': weather.get('swellDirection', {}).get('sg'),
            'swellPeriod_sg': weather.get('swellPeriod', {}).get('sg'),
            'secondarySwellHeight_sg': weather.get('secondarySwellHeight', {}).get('sg'),
            'secondarySwellDirection_sg': weather.get('secondarySwellDirection', {}).get('sg'),
            'secondarySwellPeriod_sg': weather.get('secondarySwellPeriod', {}).get('sg'),
            'windSpeed_sg': weather.get('windSpeed', {}).get('sg'),
            'windDirection_sg': weather.get('windDirection', {}).get('sg'),
            'waterTemperature_sg': weather.get('waterTemperature', {}).get('sg'),
            'airTemperature_sg': weather.get('airTemperature', {}).get('sg'),
            'currentSpeed_sg': weather.get('currentSpeed', {}).get('sg'),
            'currentDirection_sg': weather.get('currentDirection', {}).get('sg'),
            'seaLevel_sg': sea_level_entry.get('sg'),
            'tide_type': sea_level_entry.get('tide_type') # Campo calculado
        })

    merged.sort(key=lambda x: x['time'])

    save_json_data(merged, output_filename, TREATED_DIR)
    print(f"Dados mesclados e salvos em: {output_filename}")
    return merged
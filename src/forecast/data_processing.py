import arrow
from src.utils.config import REQUEST_DIR, TREATED_DIR
from src.utils.utils import load_json_data, save_json_data
from src.utils.utils import determine_tide_phase

def filter_forecast_time(data):
    filtered = []
    for entry in data:
        try:
            hour = arrow.get(entry['time']).hour
            if 5 <= hour <= 17:
                filtered.append(entry)
        except Exception as e:
            print(f"Erro ao filtrar horário: {entry.get('time')} | {e}")
    return filtered

def merge_stormglass_data(weather_filename, sea_level_filename, output_filename):
    weather_data = load_json_data(weather_filename, REQUEST_DIR)
    sea_level_data = load_json_data(sea_level_filename, REQUEST_DIR)

    if not weather_data or 'hours' not in weather_data or not sea_level_data or 'data' not in sea_level_data:
        print("Dados inválidos para merge.")
        return None

    # Adiciona o tipo de maré aos dados de nível do mar
    sea_level_with_tide_type = determine_tide_phase(sea_level_data['data'])

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
            'tide_type': sea_level_entry.get('tide_type') # Novo campo
        })

    merged.sort(key=lambda x: x['time'])

    try:
        save_json_data(merged, output_filename, TREATED_DIR)
        print(f"Merged data saved to: {output_filename}")
        return merged
    except Exception as e:
        print(f"Erro ao salvar merged data: {e}")
        return None
import arrow
import os
import json
import decimal
from collections import defaultdict

def load_json_data(filename, directory):
    """
    Carrega dados JSON a partir de um arquivo localizado no diretório especificado.
    """
    path = os.path.join(directory, filename)
    if not os.path.exists(path):
        print(f"Arquivo não encontrado: {path}")
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar JSON de {path}: {e}")
        return None

def save_json_data(data, filename, directory):
    """
    Salva dados em formato JSON no diretório especificado.
    Cria o diretório se ele não existir.
    """
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, filename)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f"Erro ao salvar JSON em {path}: {e}")
        raise e

def convert_to_localtime(data, timezone='America/Sao_Paulo'):
    for entry in data:
        try:
            local_time = arrow.get(entry['time']).to(timezone)
            entry['time'] = local_time.isoformat()
        except Exception as e:
            print(f"Erro ao converter horário: {entry.get('time')} | {e}")
    return data

def convert_to_localtime_string(timestamp_str, timezone='America/Sao_Paulo'):
    """Converte um timestamp string UTC para uma string no fuso horário local e formata."""
    if not timestamp_str:
        return ""
    try:
        utc_time = arrow.get(timestamp_str).to('utc')
        local_time = utc_time.to(timezone)
        return local_time.format('YYYY-MM-DD HH:mm:ss ZZZ')
    except Exception as e:
        print(f"Erro ao converter string de horário '{timestamp_str}' para horário local: {e}")
        return ""

def load_config(file_path='config.json'):
    """Carrega as configurações de um arquivo JSON."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Erro: O arquivo de configuração '{file_path}' não foi encontrado.")
        return None
    except json.JSONDecodeError:
        print(f"Erro: O arquivo '{file_path}' não é um JSON válido.")
        return None
    except Exception as e:
        print(f"Erro ao carregar o arquivo de configuração '{file_path}': {e}")
        return None

def save_config(config_data, file_path='config.json'):
    """Salva as configurações em um arquivo JSON."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=4)
        print(f"Configuração salva em '{file_path}'.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo de configuração '{file_path}': {e}")

def get_cardinal_direction(degrees):
    """
    Converts degrees (0-360) to a cardinal or intercardinal direction.
    Handles decimal.Decimal input by converting to float.
    """
    if degrees is None:
        return "N/A"
    
    # Adicionar esta linha para garantir que 'degrees' seja um float
    if isinstance(degrees, decimal.Decimal):
        degrees = float(degrees)

    directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                  "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    
    # Adiciona 11.25 para ajustar o ponto de partida (N é de 348.75 a 11.25)
    index = int((degrees + 11.25) / 22.5) % 16
    return directions[index]

def cardinal_to_degrees(cardinal_direction_str):
    """Converts a cardinal direction string to its numerical degree representation (0-360)."""
    if cardinal_direction_str is None:
        return None
    mapping = {
        'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5,
        'E': 90, 'ESE': 112.5, 'SE': 135, 'SSE': 157.5,
        'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
        'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
    }
    return mapping.get(cardinal_direction_str.upper(), None)
    
def determine_tide_phase(sea_level_data):
    """
    Determina a fase da maré (e.g., 'low', 'high', 'rising', 'falling')
    para uma lista de dados de nível do mar.
    
    Lida corretamente com:
    - Picos e vales de maré (high/low)
    - Picos "flats" (mesmo nível por múltiplas horas)
    - Descontinuidades entre dias (dados apenas diurnos)
    - Tendências de subida e descida
    """
    def _group_by_date(data):
        """Agrupa dados por dia para tratar descontinuidades."""
        daily_groups = defaultdict(list)
        for entry in data:
            date_str = arrow.get(entry['time']).format('YYYY-MM-DD')
            daily_groups[date_str].append(entry)
        return daily_groups
    
    def _find_previous_different_level(data, current_index):
        """Encontra o índice e nível do ponto anterior com nível diferente."""
        current_level = data[current_index].get('sg')
        
        for i in range(current_index - 1, -1, -1):
            prev_level = data[i].get('sg')
            if prev_level is not None and prev_level != current_level:
                return i, prev_level
        
        return None, None
    
    def _find_next_different_level(data, current_index):
        """Encontra o índice e nível do ponto posterior com nível diferente."""
        current_level = data[current_index].get('sg')
        
        for i in range(current_index + 1, len(data)):
            next_level = data[i].get('sg')
            if next_level is not None and next_level != current_level:
                return i, next_level
        
        return None, None
    
    def _classify_tide_point(data, index):
        """Determina a fase da maré para um ponto específico."""
        current_level = data[index].get('sg')
        
        if current_level is None:
            return 'unknown'
        
        # Busca vizinhos com níveis diferentes (para tratar picos flats)
        prev_index, prev_level = _find_previous_different_level(data, index)
        next_index, next_level = _find_next_different_level(data, index)
        
        # Classifica baseado nos vizinhos encontrados
        if prev_level is not None and next_level is not None:
            # Caso normal: temos vizinhos em ambos os lados
            if current_level > prev_level and current_level > next_level:
                return 'high'  # Pico
            elif current_level < prev_level and current_level < next_level:
                return 'low'   # Vale
            elif current_level >= prev_level:
                return 'rising'  # Subindo
            else:
                return 'falling' # Descendo
        
        elif prev_level is None and next_level is not None:
            # Primeiro ponto do dia
            if current_level < next_level:
                return 'rising'
            elif current_level > next_level:
                return 'falling'
            else:
                return 'rising'  # Mesmo nível - assume rising
        
        elif prev_level is not None and next_level is None:
            # Último ponto do dia
            if current_level > prev_level:
                return 'rising'
            elif current_level < prev_level:
                return 'falling'
            else:
                return 'falling'  # Mesmo nível - assume falling
        
        else:
            # Apenas um ponto
            return 'unknown'
    
    def _process_daily_data(daily_data):
        """Processa os dados de maré de um único dia."""
        if not daily_data:
            return []
        
        # Ordena os dados do dia por tempo
        daily_data = sorted(daily_data, key=lambda x: x['time'])
        
        for i, entry in enumerate(daily_data):
            tide_type = _classify_tide_point(daily_data, i)
            entry['tide_type'] = tide_type
        
        return daily_data
    
    # Função principal
    if not sea_level_data:
        return []

    # Ordena os dados por tempo
    sorted_data = sorted(sea_level_data, key=lambda x: x['time'])
    
    # Agrupa dados por dia para tratar descontinuidades
    daily_groups = _group_by_date(sorted_data)
    
    result = []
    
    # Processa cada dia separadamente
    for date_str in sorted(daily_groups.keys()):
        daily_data = daily_groups[date_str]
        processed_daily = _process_daily_data(daily_data)
        result.extend(processed_daily)
    
    # Reordena o resultado final por tempo e retorna
    return sorted(result, key=lambda x: x['time'])
import json
import os
import sys
import arrow # Adicione a importação do arrow

# Adiciona o diretório 'src' ao path para que possamos importar os módulos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from utils.utils import determine_tide_phase, load_json_data
from utils.config import REQUEST_DIR

def run_test():
    """
    Lê o arquivo sea_level_data.json da pasta data/requests,
    processa-o com a função determine_tide_phase e salva
    o resultado em um novo arquivo JSON para análise.
    """
    print("Iniciando teste da lógica de maré...")

    # 1. Carregar os dados de entrada
    sea_level_data = load_json_data('sea_level_data.json', REQUEST_DIR)

    if not sea_level_data or 'data' not in sea_level_data:
        print(f"ERRO: Não foi possível carregar os dados de 'sea_level_data.json' do diretório '{REQUEST_DIR}'.")
        print("Certifique-se de que o arquivo existe e contém a chave 'data'.")
        return

    # 2. Executar a função que queremos testar
    print(f"Processando {len(sea_level_data['data'])} registros de nível do mar...")
    processed_data = determine_tide_phase(sea_level_data['data'])

    # 3. Formatar a saída para facilitar a análise
    analysis_output = []
    for entry in processed_data:
        # Formata o timestamp para um formato mais legível (Dia/Mês HH:MM)
        formatted_time = arrow.get(entry.get('time')).format('DD/MM HH:mm')
        
        analysis_output.append({
            "timestamp": formatted_time, # ADICIONADO
            "sea_level": entry.get('sg'),
            "calculated_tide_type": entry.get('tide_type')
        })

    # 4. Salvar o resultado em um novo arquivo
    output_filename = 'tide_phase_analysis_result.json'
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(analysis_output, f, ensure_ascii=False, indent=4)
        print(f"\nSUCESSO! O resultado da análise foi salvo em: {output_filename}")
        print("Abra este arquivo para verificar os resultados da função.")
    except Exception as e:
        print(f"\nERRO: Falha ao salvar o arquivo de resultado: {e}")


if __name__ == "__main__":
    run_test()
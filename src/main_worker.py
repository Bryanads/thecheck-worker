# src/main_worker.py

import asyncio
import datetime
import json
import requests
from collections import defaultdict
from typing import List, Dict, Any, Optional

# --- Importações (sem alterações) ---
from src.db.connection import init_async_db_pool, close_db_pool
from src.db import queries as worker_queries
from src.services.scoring_service import calculate_overall_score
from src.forecast.data_processing import merge_stormglass_data
from src.utils.config import (
    STORMGLASS_API_KEYS, FORECAST_DAYS, WEATHER_API_URL,
    TIDE_SEA_LEVEL_API_URL, PARAMS_WEATHER_API
)

# --- Funções Auxiliares e de Requisição (sem alterações) ---
async def fetch_data_async(api_url: str, params: Dict, api_key: str, label: str) -> Optional[Dict]:
    print(f"Buscando dados de {label} com a chave terminada em '...{api_key[-4:]}'")
    headers = {'Authorization': api_key}
    loop = asyncio.get_event_loop()
    try:
        response = await loop.run_in_executor(
            None, lambda: requests.get(api_url, headers=headers, params=params, timeout=20)
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"ERRO ao buscar dados de {label}: {e}")
        return None

async def get_preferences_for_user_and_spot(user_id: str, spot_id: int, user_profile: Dict, user_prefs_list: List[Dict]) -> Dict[str, Any]:
    surf_level = user_profile.get('surf_level', 'intermediario')
    final_prefs = await worker_queries.get_generic_preferences_by_level(surf_level)
    spot_level_prefs = await worker_queries.get_spot_level_preferences(spot_id, surf_level)
    if spot_level_prefs:
        final_prefs.update({k: v for k, v in spot_level_prefs.items() if v is not None})
    user_spot_prefs = next((p for p in user_prefs_list if p['spot_id'] == spot_id and p.get('is_active')), None)
    if user_spot_prefs:
        final_prefs.update({k: v for k, v in user_spot_prefs.items() if v is not None})
    return final_prefs

# --- Tarefa 1 (sem alterações) ---
async def process_spot_forecast(spot_details: Dict, api_key: str):
    spot_id, spot_name, latitude, longitude = spot_details['spot_id'], spot_details['name'], spot_details['latitude'], spot_details['longitude']
    print(f"\n{'='*20} PROCESSANDO: {spot_name} (ID: {spot_id}) {'='*20}")
    start = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0)
    end = start + datetime.timedelta(days=FORECAST_DAYS)
    weather_params = {'lat': latitude, 'lng': longitude, 'params': ','.join(PARAMS_WEATHER_API), 'start': int(start.timestamp()), 'end': int(end.timestamp())}
    sea_level_params = {'lat': latitude, 'lng': longitude, 'start': int(start.timestamp()), 'end': int(end.timestamp())}
    weather_data, sea_level_data = await asyncio.gather(
        fetch_data_async(WEATHER_API_URL, weather_params, api_key, f"Tempo para {spot_name}"),
        fetch_data_async(TIDE_SEA_LEVEL_API_URL, sea_level_params, api_key, f"Nível do mar para {spot_name}")
    )
    if not weather_data or 'hours' not in weather_data or not sea_level_data or 'data' not in sea_level_data:
        print(f"ERRO: Falha ao buscar dados essenciais para {spot_name}. Pulando inserção.")
        return
    merged = merge_stormglass_data(weather_data, sea_level_data)
    if not merged:
        print(f"ERRO: Falha ao mesclar dados para {spot_name}. Pulando inserção.")
        return
    await worker_queries.insert_forecast_data(spot_id, merged)
    print(f"--- SUCESSO: Dados para {spot_name} (ID: {spot_id}) processados e inseridos. ---")

async def update_all_forecasts():
    print("--- INICIANDO TAREFA 1: ATUALIZAÇÃO DE PREVISÕES ---")
    if not STORMGLASS_API_KEYS:
        print("ERRO CRÍTICO: Nenhuma chave de API da Stormglass encontrada.")
        return
    all_spots = await worker_queries.get_all_spots()
    if not all_spots:
        print("Nenhum spot encontrado. Abortando.")
        return
    print(f"Encontrados {len(all_spots)} spots para atualizar usando {len(STORMGLASS_API_KEYS)} chaves.")
    tasks = [process_spot_forecast(spot, STORMGLASS_API_KEYS[i % len(STORMGLASS_API_KEYS)]) for i, spot in enumerate(all_spots)]
    await asyncio.gather(*tasks)
    print("\n--- TAREFA 1 CONCLUÍDA: TODAS AS PREVISÕES FORAM ATUALIZADAS ---")

# --- Tarefa 2: Com a Lógica de Formatação Corrigida ---

async def calculate_and_save_for_config(
    user_id: str, user_profile: Dict, user_prefs_list: List[Dict],
    spot_ids: List[int], day_offsets: List[int], time_window: tuple,
    cache_key: str
):
    """Função reutilizável para calcular e salvar um conjunto de recomendações."""
    print(f"  - Calculando para config: '{cache_key}'...")
    start_utc = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_utc = start_utc + datetime.timedelta(days=(max(day_offsets) + 1))

    daily_options = defaultdict(list)
    for spot_id in spot_ids:
        spot_details, spot_forecasts = await asyncio.gather(
            worker_queries.get_spot_by_id(spot_id),
            worker_queries.get_forecasts_for_spot(spot_id, start_utc, end_utc)
        )
        user_prefs = await get_preferences_for_user_and_spot(user_id, spot_id, user_profile, user_prefs_list)
        if not spot_details or not spot_forecasts: continue

        for forecast in spot_forecasts:
            forecast_date = forecast['timestamp_utc'].date()
            if (forecast_date - start_utc.date()).days in day_offsets and \
               time_window[0] <= forecast['timestamp_utc'].time() <= time_window[1]:
                score_data = await calculate_overall_score(forecast, user_prefs, spot_details, user_profile)
                if score_data['overall_score'] > 30:
                    daily_options[forecast_date].append({
                        "spot_id": spot_id, "spot_name": spot_details['name'],
                        "timestamp_utc": forecast['timestamp_utc'],
                        "forecast_conditions": forecast, **score_data
                    })

    # --- LÓGICA DE FORMATAÇÃO QUE ESTAVA FALTANDO ---
    final_response = []
    for date, hourly_recs in sorted(daily_options.items()):
        best_spot_sessions = {}
        for rec in hourly_recs:
            sid = rec['spot_id']
            if sid not in best_spot_sessions or rec['overall_score'] > best_spot_sessions[sid]['best_overall_score']:
                best_spot_sessions[sid] = {
                    "spot_id": sid, "spot_name": rec['spot_name'],
                    "best_hour_utc": rec['timestamp_utc'], "best_overall_score": rec['overall_score'],
                    "detailed_scores": rec['detailed_scores'], "forecast_conditions": rec['forecast_conditions']
                }
        if not best_spot_sessions: continue
        ranked_spots = sorted(best_spot_sessions.values(), key=lambda x: x['best_overall_score'], reverse=True)
        final_response.append({"date": date, "ranked_spots": ranked_spots})
    # --- FIM DA LÓGICA FALTANTE ---

    if final_response:
        await worker_queries.save_recommendation_cache(user_id, cache_key, final_response)
        print(f"    -> Cache para '{cache_key}' salvo com sucesso.")
    else:
        print(f"    -> Nenhuma recomendação encontrada para '{cache_key}'.")


async def calculate_all_user_recommendations():
    print("\n--- INICIANDO TAREFA 2: CÁLCULO DE SCORES PERSONALIZADOS ---")
    users_to_process = await worker_queries.get_all_active_users_with_presets()
    if not users_to_process:
        print("Nenhum usuário com presets encontrado.")
        return
    print(f"Encontrados {len(users_to_process)} usuários para processar.")

    for user_job in users_to_process:
        user_id = user_job['user_id']
        print(f"\nProcessando recomendações para o usuário: {user_id}")

        user_profile, user_prefs_list = await worker_queries.get_full_user_details(user_id)
        if not user_profile:
            print(f"  - Perfil não encontrado. Pulando.")
            continue

        # O worker precisa da função weekdays_to_offsets
        day_selection_values = user_job['day_selection_values']
        preset_offsets = day_selection_values
        if user_job['day_selection_type'] == 'weekdays':
            # Simples adaptação da função da API
            today = (datetime.datetime.now(datetime.timezone.utc).weekday() + 1) % 7
            preset_offsets = [i for i in range(7) if ((today + i) % 7) in day_selection_values] or [0]

        configs = {
            "today": {"day_offsets": [0]},
            "tomorrow": {"day_offsets": [1]},
            user_job['name']: {"day_offsets": preset_offsets}
        }

        for key, config in configs.items():
            await calculate_and_save_for_config(
                user_id, user_profile, user_prefs_list,
                spot_ids=user_job['spot_ids'],
                day_offsets=config['day_offsets'],
                time_window=(user_job['start_time'], user_job['end_time']),
                cache_key=key
            )
    print("\n--- TAREFA 2 CONCLUÍDA: SCORES PERSONALIZADOS CALCULADOS ---")


# --- Orquestrador Principal (main) ---
async def main():
    await init_async_db_pool()
    try:
        await update_all_forecasts()
        await calculate_all_user_recommendations()
        await worker_queries.delete_old_forecast_data(7)
    except Exception as e:
        print(f"\nERRO CRÍTICO NO WORKER: {e}")
    finally:
        await close_db_pool()
        print("\nCiclo do worker concluído.")

if __name__ == "__main__":
    print("Iniciando ciclo do TheCheck Worker...")
    asyncio.run(main())
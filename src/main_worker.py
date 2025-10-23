import asyncio
import datetime
import json
import requests
from collections import defaultdict
from typing import List, Dict, Any, Optional
import traceback # Import traceback for detailed error logging

# --- Importações ---
from src.db.connection import init_async_db_pool, close_db_pool
from src.db import queries as worker_queries
from src.services.scoring_service import calculate_overall_score
from src.forecast.data_processing import merge_stormglass_data # A função corrigida será usada aqui
from src.utils.config import (
    STORMGLASS_API_KEYS, FORECAST_DAYS, WEATHER_API_URL,
    TIDE_SEA_LEVEL_API_URL, PARAMS_WEATHER_API
)

# --- Funções Auxiliares e de Requisição ---
async def fetch_data_async(api_url: str, params: Dict, api_key: str, label: str) -> Optional[Dict]:
    print(f"Buscando dados de {label} com a chave terminada em '...{api_key[:4]}'")
    headers = {'Authorization': api_key}
    loop = asyncio.get_event_loop()
    try:
        # Increased timeout to 30 seconds
        response = await loop.run_in_executor(
            None, lambda: requests.get(api_url, headers=headers, params=params, timeout=30)
        )
        response.raise_for_status()
        # Handle potential empty response body for non-200 but ok statuses (like 204)
        if response.status_code == 204:
            print(f"AVISO: Recebido status 204 (No Content) de {label}. Retornando None.")
            return None
        # Attempt to parse JSON, handle potential errors
        try:
            return response.json()
        except json.JSONDecodeError as json_err:
            print(f"ERRO ao decodificar JSON de {label}: {json_err}. Conteúdo: {response.text[:500]}") # Log first 500 chars
            return None
    except requests.exceptions.Timeout:
        print(f"ERRO: Timeout ao buscar dados de {label} após 30 segundos.")
        return None
    except requests.exceptions.RequestException as e:
        # Log more details about the request error
        print(f"ERRO ao buscar dados de {label}: {e}")
        if e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Response Body: {e.response.text[:500]}") # Log first 500 chars
        return None
    except Exception as general_err: # Catch any other unexpected errors
        print(f"ERRO inesperado ao buscar dados de {label}: {general_err}")
        traceback.print_exc() # Print full traceback for unexpected errors
        return None


async def get_preferences_for_user_and_spot(user_id: str, spot_id: int, user_profile: Dict, user_prefs_list: List[Dict]) -> Dict[str, Any]:
    surf_level = user_profile.get('surf_level', 'intermediario')
    # Start with generic level preferences
    final_prefs = await worker_queries.get_generic_preferences_by_level(surf_level)
    # Layer spot-specific level preferences (overrides generic level prefs)
    spot_level_prefs = await worker_queries.get_spot_level_preferences(spot_id, surf_level)
    if spot_level_prefs:
        final_prefs.update({k: v for k, v in spot_level_prefs.items() if v is not None})
    # Layer user's specific preferences for this spot (overrides level prefs if active)
    user_spot_prefs = next((p for p in user_prefs_list if p['spot_id'] == spot_id and p.get('is_active')), None)
    if user_spot_prefs:
        final_prefs.update({k: v for k, v in user_spot_prefs.items() if v is not None and k != 'is_active'}) # Exclude is_active itself
    return final_prefs

# --- Tarefa 1: Atualização de Previsões ---
async def process_spot_forecast(spot_details: Dict, api_key: str):
    # Ensure required spot details are present
    spot_id = spot_details.get('spot_id')
    spot_name = spot_details.get('name', f"Spot Desconhecido (ID: {spot_id})")
    latitude = spot_details.get('latitude')
    longitude = spot_details.get('longitude')

    if not all([spot_id, latitude, longitude]):
        print(f"ERRO: Detalhes incompletos para o spot: {spot_details}. Pulando.")
        return

    print(f"\n{'='*20} PROCESSANDO: {spot_name} (ID: {spot_id}) {'='*20}")

    # Define start and end times in UTC
    start_utc = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_utc = start_utc + datetime.timedelta(days=FORECAST_DAYS)

    # Prepare parameters for API calls
    weather_params = {
        'lat': latitude,
        'lng': longitude,
        'params': ','.join(PARAMS_WEATHER_API),
        'start': int(start_utc.timestamp()),
        'end': int(end_utc.timestamp())
    }
    sea_level_params = {
        'lat': latitude,
        'lng': longitude,
        'start': int(start_utc.timestamp()),
        'end': int(end_utc.timestamp())
    }

    # Fetch data concurrently
    weather_data, sea_level_data = await asyncio.gather(
        fetch_data_async(WEATHER_API_URL, weather_params, api_key, f"Tempo para {spot_name}"),
        fetch_data_async(TIDE_SEA_LEVEL_API_URL, sea_level_params, api_key, f"Nível do mar para {spot_name}")
    )

    # Validate fetched data before merging
    if not weather_data or 'hours' not in weather_data or not isinstance(weather_data['hours'], list):
        print(f"ERRO: Dados de tempo inválidos ou ausentes para {spot_name}. Pulando merge e inserção.")
        return
    if not sea_level_data or 'data' not in sea_level_data or not isinstance(sea_level_data['data'], list):
        print(f"ERRO: Dados de nível do mar inválidos ou ausentes para {spot_name}. Pulando merge e inserção.")
        return

    # Merge the data (using the corrected function that accepts dicts)
    # No output_filename needed here as we process in memory
    merged = merge_stormglass_data(weather_data, sea_level_data)

    if not merged:
        print(f"ERRO: Falha ao mesclar dados para {spot_name}. Pulando inserção.")
        return

    # Insert merged data into the database
    try:
        await worker_queries.insert_forecast_data(spot_id, merged)
        print(f"--- SUCESSO: Dados para {spot_name} (ID: {spot_id}) processados e inseridos. ---")
    except Exception as db_err:
        print(f"ERRO ao inserir dados no banco para {spot_name} (ID: {spot_id}): {db_err}")
        traceback.print_exc()


async def update_all_forecasts():
    print("--- INICIANDO TAREFA 1: ATUALIZAÇÃO DE PREVISÕES ---")
    if not STORMGLASS_API_KEYS:
        print("ERRO CRÍTICO: Nenhuma chave de API da Stormglass encontrada.")
        return

    try:
        all_spots = await worker_queries.get_all_spots()
    except Exception as e:
        print(f"ERRO ao buscar spots do banco de dados: {e}")
        traceback.print_exc()
        return

    if not all_spots:
        print("Nenhum spot encontrado no banco de dados. Abortando Tarefa 1.")
        return

    print(f"Encontrados {len(all_spots)} spots para atualizar usando {len(STORMGLASS_API_KEYS)} chaves.")
    # Create tasks for processing each spot
    tasks = []
    for i, spot in enumerate(all_spots):
        api_key = STORMGLASS_API_KEYS[i % len(STORMGLASS_API_KEYS)] # Rotate API keys
        tasks.append(process_spot_forecast(spot, api_key))

    # Run tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Log any exceptions that occurred during task execution
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            spot_name = all_spots[i].get('name', f"Spot ID {all_spots[i].get('spot_id')}")
            print(f"ERRO durante o processamento do spot {spot_name}: {result}")
            # Optionally log the full traceback for exceptions
            # traceback.print_exception(type(result), result, result.__traceback__)

    print("\n--- TAREFA 1 CONCLUÍDA: TODAS AS PREVISÕES FORAM ATUALIZADAS (ou tentativas foram feitas) ---")


# --- Tarefa 2: Cálculo de Scores Personalizados ---

async def calculate_and_save_for_config(
    user_id: str, user_profile: Dict, user_prefs_list: List[Dict],
    spot_ids: List[int], day_offsets: List[int], time_window: tuple,
    cache_key: str
):
    """Calcula e salva recomendações para uma configuração específica (preset, hoje, amanhã)."""
    print(f"  - Calculando para config: '{cache_key}' (Dias: {day_offsets}, Spots: {spot_ids})...")

    # Validate inputs
    if not spot_ids:
        print(f"    -> Aviso: Lista de spot_ids vazia para '{cache_key}'. Pulando.")
        return
    if not day_offsets:
        print(f"    -> Aviso: Lista de day_offsets vazia para '{cache_key}'. Pulando.")
        return
    if not isinstance(time_window, tuple) or len(time_window) != 2 or not all(isinstance(t, datetime.time) for t in time_window):
         print(f"    -> ERRO: time_window inválido para '{cache_key}'. Esperado (time, time), recebido: {time_window}. Pulando.")
         return


    start_utc = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    try:
        # Calculate end_utc based on the maximum offset required
        end_utc = start_utc + datetime.timedelta(days=(max(day_offsets) + 1))
    except ValueError: # Handle empty day_offsets case although checked above
         print(f"    -> ERRO: Lista de day_offsets inválida ou vazia para '{cache_key}'. Pulando.")
         return


    daily_options = defaultdict(list)
    processed_spots = 0

    # Fetch forecasts and calculate scores for each spot
    for spot_id in spot_ids:
        try:
            spot_details, spot_forecasts = await asyncio.gather(
                worker_queries.get_spot_by_id(spot_id),
                worker_queries.get_forecasts_for_spot(spot_id, start_utc, end_utc)
            )

            # Get combined preferences for this user/spot
            user_prefs = await get_preferences_for_user_and_spot(user_id, spot_id, user_profile, user_prefs_list)

            if not spot_details:
                print(f"    -> Aviso: Detalhes não encontrados para spot ID {spot_id}. Pulando.")
                continue
            if not spot_forecasts:
                # print(f"    -> Info: Nenhuma previsão encontrada para spot ID {spot_id} no período solicitado.")
                continue # It's normal not to have forecasts for all days requested

            processed_spots += 1
            spot_name = spot_details.get('name', f"Spot {spot_id}")

            # Iterate through hourly forecasts for the spot
            for forecast in spot_forecasts:
                forecast_dt_utc = forecast.get('timestamp_utc')
                if not forecast_dt_utc:
                    print(f"    -> Aviso: Previsão sem timestamp_utc para spot {spot_id}. Pulando hora.")
                    continue

                forecast_date = forecast_dt_utc.date()
                forecast_time_utc = forecast_dt_utc.time()
                current_day_offset = (forecast_date - start_utc.date()).days

                # Check if the forecast falls within the desired days and time window
                if current_day_offset in day_offsets and time_window[0] <= forecast_time_utc <= time_window[1]:

                    # Calculate scores for this specific hour
                    try:
                        score_data = await calculate_overall_score(forecast, user_prefs, spot_details, user_profile)
                    except Exception as score_err:
                        print(f"    -> ERRO ao calcular score para {spot_name} às {forecast_dt_utc}: {score_err}")
                        traceback.print_exc()
                        continue # Skip this hour if scoring fails

                    # Add to potential recommendations if score is above threshold
                    if score_data.get('overall_score', 0) > 30:
                        daily_options[forecast_date].append({
                            "spot_id": spot_id,
                            "spot_name": spot_name,
                            "timestamp_utc": forecast_dt_utc,
                            "forecast_conditions": forecast, # Include full forecast data
                            **score_data # Includes overall_score and detailed_scores
                        })

        except Exception as spot_proc_err:
            print(f"    -> ERRO ao processar spot ID {spot_id} para '{cache_key}': {spot_proc_err}")
            traceback.print_exc()
            continue # Continue to the next spot if one fails

    print(f"    -> Processados forecasts para {processed_spots}/{len(spot_ids)} spots.")

    # --- FORMAT AND SAVE RESULTS ---
    final_response = []
    # Sort dates to ensure consistent order in cache
    for date in sorted(daily_options.keys()):
        hourly_recs = daily_options[date]
        if not hourly_recs: continue # Skip if no valid recs for this date

        # Find the best hour for each spot on this date
        best_spot_sessions = {}
        for rec in hourly_recs:
            sid = rec['spot_id']
            # If spot not seen yet, or current hour has better score, update best session
            if sid not in best_spot_sessions or rec['overall_score'] > best_spot_sessions[sid]['best_overall_score']:
                best_spot_sessions[sid] = {
                    "spot_id": sid,
                    "spot_name": rec['spot_name'],
                    "best_hour_utc": rec['timestamp_utc'], # Store the timestamp object directly
                    "best_overall_score": rec['overall_score'],
                    "detailed_scores": rec['detailed_scores'],
                    "forecast_conditions": rec['forecast_conditions'] # Store conditions for the best hour
                }

        if not best_spot_sessions: continue # Skip day if no spots had valid scores

        # Rank spots for the day based on their best score
        ranked_spots = sorted(best_spot_sessions.values(), key=lambda x: x['best_overall_score'], reverse=True)

        # Convert datetime objects to ISO strings *before* saving to cache
        for spot_summary in ranked_spots:
             if isinstance(spot_summary['best_hour_utc'], datetime.datetime):
                 spot_summary['best_hour_utc'] = spot_summary['best_hour_utc'].isoformat()
             # Convert forecast conditions timestamps too
             fc = spot_summary.get('forecast_conditions', {})
             if fc and isinstance(fc.get('timestamp_utc'), datetime.datetime):
                  fc['timestamp_utc'] = fc['timestamp_utc'].isoformat()


        # Format date as string for JSON compatibility
        final_response.append({"date": date.isoformat(), "ranked_spots": ranked_spots})

    # Save to cache if recommendations were found
    if final_response:
        try:
            await worker_queries.save_recommendation_cache(user_id, cache_key, final_response)
            print(f"    -> Cache para '{cache_key}' salvo com sucesso ({len(final_response)} dias).")
        except Exception as cache_err:
            print(f"    -> ERRO ao salvar cache para '{cache_key}': {cache_err}")
            traceback.print_exc()
    else:
        print(f"    -> Nenhuma recomendação encontrada para '{cache_key}'. Cache não salvo.")


async def calculate_all_user_recommendations():
    print("\n--- INICIANDO TAREFA 2: CÁLCULO DE SCORES PERSONALIZADOS ---")
    try:
        users_to_process = await worker_queries.get_all_active_users_with_presets()
    except Exception as e:
        print(f"ERRO ao buscar usuários com presets: {e}")
        traceback.print_exc()
        return # Cannot proceed without user list

    if not users_to_process:
        print("Nenhum usuário ativo com presets encontrado para processar.")
        return
    print(f"Encontrados {len(users_to_process)} usuários ativos com presets para processar.")

    processed_user_count = 0
    # Process recommendations for each user
    for user_job in users_to_process:
        user_id = user_job.get('user_id')
        preset_name = user_job.get('name', 'Preset Desconhecido')
        if not user_id:
             print("  - Aviso: Encontrado job de usuário sem user_id. Pulando.")
             continue

        print(f"\nProcessando recomendações para o usuário: {user_id} (Preset: '{preset_name}')")

        try:
            # Fetch user profile and preferences list
            user_profile, user_prefs_list = await worker_queries.get_full_user_details(user_id)
            if not user_profile:
                print(f"  - Aviso: Perfil não encontrado para usuário {user_id}. Pulando.")
                continue

            # --- CORRIGIDA: LÓGICA DE WEEKDAYS PARA OFFSETS ---
            day_selection_type = user_job.get('day_selection_type')
            day_selection_values = user_job.get('day_selection_values', [])
            preset_offsets = []

            if day_selection_type == 'offsets':
                # Filter for valid non-negative integer offsets
                preset_offsets = [int(v) for v in day_selection_values if isinstance(v, (int, float)) and v >= 0]
            elif day_selection_type == 'weekdays':
                 # Convert frontend weekdays (0=Sun) to Python's weekday() standard (0=Mon, 6=Sun)
                 # Frontend 0 (Sun) -> Python 6
                 # Frontend 1 (Mon) -> Python 0
                 # ...
                 # Frontend 6 (Sat) -> Python 5
                python_weekdays = { (d - 1 + 7) % 7 if d > 0 else 6 for d in day_selection_values if isinstance(d, int) and 0 <= d <= 6 } # Use set for efficiency

                if not python_weekdays:
                     print(f"  -> Aviso: Valores de weekdays inválidos para preset '{preset_name}'. Usando offset 0.")
                else:
                    today_utc_weekday = datetime.datetime.now(datetime.timezone.utc).weekday() # 0 = Mon, ..., 6 = Sun
                    for i in range(7): # Check next 7 days (0 to 6)
                        future_day_weekday = (today_utc_weekday + i) % 7
                        if future_day_weekday in python_weekdays:
                            preset_offsets.append(i) # Add the offset if the future day matches selected weekdays
            else:
                 print(f"  -> Aviso: day_selection_type inválido ('{day_selection_type}') para preset '{preset_name}'. Usando offset 0.")


            # Ensure preset_offsets has at least today if calculation failed or resulted empty
            if not preset_offsets:
                print(f"  -> Aviso: Nenhum dia válido após cálculo para preset '{preset_name}'. Usando offset 0 (hoje).")
                preset_offsets = [0]
            # --- FIM DA CORREÇÃO ---

            # Validate spot_ids and time_window
            spot_ids = user_job.get('spot_ids', [])
            start_time = user_job.get('start_time')
            end_time = user_job.get('end_time')

            if not spot_ids:
                 print(f"  -> Aviso: Nenhum spot_id encontrado para preset '{preset_name}'. Pulando cálculo para este preset.")
                 continue
            if not isinstance(start_time, datetime.time) or not isinstance(end_time, datetime.time):
                 print(f"  -> ERRO: start_time ou end_time inválidos para preset '{preset_name}'. Recebido start: {start_time}, end: {end_time}. Pulando.")
                 continue

            # Define configurations to calculate (today, tomorrow, and the user's default preset)
            configs = {
                "today": {"day_offsets": [0]},
                "tomorrow": {"day_offsets": [1]},
                preset_name: {"day_offsets": preset_offsets} # Use calculated offsets for the preset
            }

            # Calculate and save for each configuration
            config_tasks = []
            for key, config in configs.items():
                # Ensure day_offsets is not empty before creating the task
                if config['day_offsets']:
                    config_tasks.append(
                        calculate_and_save_for_config(
                            user_id, user_profile, user_prefs_list,
                            spot_ids=spot_ids,
                            day_offsets=config['day_offsets'],
                            time_window=(start_time, end_time),
                            cache_key=key
                        )
                    )
                else:
                     print(f"    -> Aviso: Configuração '{key}' pulada devido a day_offsets vazio.")


            if config_tasks:
                await asyncio.gather(*config_tasks)
            else:
                print(f"  -> Nenhuma configuração válida para calcular para o usuário {user_id}.")

            processed_user_count += 1

        except Exception as user_proc_err:
            print(f"  -> ERRO CRÍTICO ao processar usuário {user_id} (Preset: '{preset_name}'): {user_proc_err}")
            traceback.print_exc()
            # Continue to the next user even if one fails

    print(f"\n--- TAREFA 2 CONCLUÍDA: Recomendações processadas para {processed_user_count}/{len(users_to_process)} usuários ---")


# --- Orquestrador Principal (main) ---
async def main():
    start_time = datetime.datetime.now()
    print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] Iniciando ciclo do TheCheck Worker...")
    try:
        await init_async_db_pool()
        print("Pool de conexões inicializado.")

        # Executa as tarefas principais
        await update_all_forecasts()
        await calculate_all_user_recommendations()

        # Limpeza de dados antigos (executa mesmo se as tarefas anteriores falharem)
        await worker_queries.delete_old_forecast_data(7)

    except Exception as e:
        print(f"\nERRO CRÍTICO NO WORKER (ciclo principal): {e}")
        traceback.print_exc()
    finally:
        # Garante que o pool seja fechado
        await close_db_pool()
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        print(f"[{end_time.strftime('%Y-%m-%d %H:%M:%S')}] Ciclo do worker concluído. Duração: {duration}")

if __name__ == "__main__":
    # Roda o ciclo principal do worker
    asyncio.run(main())
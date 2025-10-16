# src/db/queries.py
import datetime
import json
from typing import List, Dict, Any, Optional
from src.db.connection import get_async_db_connection, release_async_db_connection

# --- (as outras funções no topo do arquivo permanecem as mesmas) ---
async def insert_forecast_data(spot_id, forecast_data):
    #... (sem alterações aqui)
    if not forecast_data:
        print("Nenhum dado horário para inserir.")
        return

    print(f"Iniciando inserção/atualização de {len(forecast_data)} previsões horárias para o spot ID: {spot_id}...")
    conn = await get_async_db_connection()
    try:
        # Usando copy_records_to_table para uma inserção em massa muito mais rápida
        columns = [
            'spot_id', 'timestamp_utc', 'wave_height_sg', 'wave_direction_sg', 'wave_period_sg',
            'swell_height_sg', 'swell_direction_sg', 'swell_period_sg', 'secondary_swell_height_sg',
            'secondary_swell_direction_sg', 'secondary_swell_period_sg', 'wind_speed_sg',
            'wind_direction_sg', 'water_temperature_sg', 'air_temperature_sg', 'current_speed_sg',
            'current_direction_sg', 'sea_level_sg', 'tide_type'
        ]

        data_to_insert = []
        for entry in forecast_data:
            record = {
                'spot_id': spot_id,
                'timestamp_utc': datetime.datetime.fromisoformat(entry['time']),
                'tide_type': entry.get('tide_type')
            }
            # Mapeia os nomes das chaves do JSON para os nomes das colunas
            for col in columns:
                if col not in record:
                    # Remove _sg e muda para camelCase para corresponder ao JSON
                    json_key = col[:-3] + 'Sg' if col.endswith('_sg') else col
                    # Garante que a chave exista antes de tentar acessá-la
                    if json_key in entry:
                         record[col] = entry.get(json_key)
                    else:
                        # Se a chave não existir no JSON, usa None como padrão
                        record[col] = None
            data_to_insert.append(record)

        # Cria uma tabela temporária, insere os dados e depois faz um "upsert" na tabela principal
        temp_table_name = f"temp_forecasts_{spot_id}"
        await conn.execute(f"CREATE TEMP TABLE {temp_table_name} (LIKE forecasts INCLUDING DEFAULTS) ON COMMIT DROP;")

        # Converte o dicionário para uma tupla na ordem correta das colunas
        records_to_copy = [tuple(d.get(col) for col in columns) for d in data_to_insert]

        await conn.copy_records_to_table(temp_table_name, records=records_to_copy, columns=columns)

        # Constrói a parte SET da query de update dinamicamente
        update_set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns if col not in ['spot_id', 'timestamp_utc'])
        update_set_clause += ", last_modified_at = NOW()"

        await conn.execute(f"""
            INSERT INTO forecasts ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table_name}
            ON CONFLICT (spot_id, timestamp_utc) DO UPDATE SET
                {update_set_clause};
        """)
    finally:
        await release_async_db_connection(conn)
    print(f"Processo de inserção/atualização para o spot {spot_id} finalizado.")


async def get_all_spots():
    conn = await get_async_db_connection()
    try:
        rows = await conn.fetch("SELECT spot_id, name, latitude, longitude, timezone, ideal_swell_direction, ideal_wind_direction, ideal_sea_level, ideal_tide_flow FROM spots ORDER BY spot_id;")
        if not rows:
            print("Nenhum spot encontrado no banco de dados.")
            return []
        return [dict(row) for row in rows]
    finally:
        await release_async_db_connection(conn)


async def delete_old_forecast_data(days_to_keep=7):
    conn = await get_async_db_connection()
    try:
        time_threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_to_keep)
        print(f"\nIniciando limpeza de dados de previsão anteriores a {time_threshold.strftime('%Y-%m-%d')}...")
        result_forecasts = await conn.execute("DELETE FROM forecasts WHERE timestamp_utc < $1", time_threshold)
        deleted_count = int(result_forecasts.split(' ')[1]) if 'DELETE' in result_forecasts else 0
        print(f"{deleted_count} registros antigos removidos da tabela 'forecasts'.")
    except Exception as e:
        print(f"Erro durante a limpeza de dados antigos: {e}")
    finally:
        await release_async_db_connection(conn)
    print("Limpeza de dados antigos finalizada.")

async def get_spot_by_id(spot_id: int) -> Optional[Dict[str, Any]]:
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow("SELECT * FROM spots WHERE spot_id = $1", spot_id)
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

async def get_forecasts_for_spot(spot_id: int, start_utc: datetime.datetime, end_utc: datetime.datetime) -> List[Dict[str, Any]]:
    conn = await get_async_db_connection()
    try:
        rows = await conn.fetch("SELECT * FROM forecasts WHERE spot_id = $1 AND timestamp_utc BETWEEN $2 AND $3 ORDER BY timestamp_utc;", spot_id, start_utc, end_utc)
        return [dict(row) for row in rows]
    finally:
        await release_async_db_connection(conn)

async def get_all_active_users_with_presets() -> List[Dict[str, Any]]:
    conn = await get_async_db_connection()
    try:
        rows = await conn.fetch("""
            WITH UserPresets AS (
                SELECT
                    p.id AS user_id, pr.name, pr.preset_id, pr.spot_ids,
                    pr.start_time, pr.end_time, pr.day_selection_type, pr.day_selection_values,
                    ROW_NUMBER() OVER(PARTITION BY p.id ORDER BY pr.is_default DESC, pr.preset_id ASC) as rn
                FROM profiles p JOIN presets pr ON p.id = pr.user_id
            )
            SELECT user_id, name, spot_ids, start_time, end_time, day_selection_type, day_selection_values
            FROM UserPresets WHERE rn = 1;
        """)
        return [{**row, 'user_id': str(row['user_id'])} for row in rows]
    finally:
        await release_async_db_connection(conn)

async def get_full_user_details(user_id: str) -> (Optional[Dict], List[Dict]):
    conn = await get_async_db_connection()
    try:
        profile = await conn.fetchrow("SELECT * FROM profiles WHERE id = $1", user_id)
        if not profile: return None, []
        prefs = await conn.fetch("SELECT * FROM user_spot_preferences WHERE user_id = $1", user_id)
        return dict(profile), [dict(p) for p in prefs]
    finally:
        await release_async_db_connection(conn)

async def get_spot_level_preferences(spot_id: int, surf_level: str) -> Optional[Dict[str, Any]]:
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow("SELECT * FROM spot_level_preferences WHERE spot_id = $1 AND surf_level = $2", spot_id, surf_level)
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

async def get_generic_preferences_by_level(surf_level: str) -> Dict[str, Any]:
    if surf_level == 'iniciante':
        return {"ideal_swell_height": 0.8, "max_swell_height": 1.2, "max_wind_speed": 4.0, "ideal_water_temperature": 24.0, "ideal_air_temperature": 26.0}
    if surf_level == 'maroleiro':
        return {"ideal_swell_height": 1.0, "max_swell_height": 1.5, "max_wind_speed": 5.0, "ideal_water_temperature": 23.0, "ideal_air_temperature": 25.0}
    if surf_level == 'pro':
        return {"ideal_swell_height": 2.2, "max_swell_height": 3.5, "max_wind_speed": 9.0, "ideal_water_temperature": 21.0, "ideal_air_temperature": 24.0}
    return {"ideal_swell_height": 1.5, "max_swell_height": 2.2, "max_wind_speed": 7.0, "ideal_water_temperature": 22.0, "ideal_air_temperature": 25.0}


# --- FUNÇÃO CORRIGIDA ---
async def save_recommendation_cache(user_id: str, cache_key: str, payload: List[Dict]):
    """Salva o resultado do cálculo de recomendação na tabela de cache."""
    conn = await get_async_db_connection()
    try:
        json_payload = json.dumps(payload, default=str)
        # Query simplificada para remover a ambígua coluna 'cache_date'
        await conn.execute(
            """
            INSERT INTO user_recommendation_cache (user_id, cache_key, recommendations_payload, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (user_id, cache_key) DO UPDATE SET
                recommendations_payload = EXCLUDED.recommendations_payload,
                created_at = NOW();
            """,
            user_id, cache_key, json_payload
        )
    finally:
        await release_async_db_connection(conn)
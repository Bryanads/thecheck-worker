
import os
import datetime
import asyncpg
from src.db.connection import get_async_db_connection, release_async_db_connection


# --- Funções Assíncronas de Escrita de Dados (INSERT/UPDATE) ---

# --- Funções de Escrita de Dados (INSERT/UPDATE) ---

async def add_spot_to_db(name, latitude, longitude, timezone):
    """
    Add a new beach (spot) to the database.
    If the beach already exists, it will not be added again.
    """
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow("SELECT spot_id FROM spots WHERE spot_name = $1", name)
        if row:
            print(f"Spot '{name}' already exists in the database.")
            return None
        new_id = await conn.fetchval(
            """
            INSERT INTO spots (spot_name, latitude, longitude, timezone)
            VALUES ($1, $2, $3, $4)
            RETURNING spot_id;
            """,
            name, latitude, longitude, timezone
        )
        print(f"Spot '{name}' (ID: {new_id}, Lat: {latitude}, Lng: {longitude}, Timezone: {timezone}) addition completed!")
        return new_id
    finally:
        await release_async_db_connection(conn)

async def insert_forecast_data(spot_id, forecast_data):
    """
    Inserts/Updates the forecast data into the forecasts table.
    Usa context manager para conexão e cursor.
    """
    if not forecast_data:
        print("No hourly data to insert.")
        return

    print(f"Starting insertion/update of {len(forecast_data)} hourly forecasts...")
    conn = await get_async_db_connection()
    try:
        for entry in forecast_data:
            timestamp_utc = datetime.datetime.fromisoformat(entry['time'])
            values_to_insert = (
                spot_id,
                timestamp_utc,
                entry.get('waveHeight_sg'),
                entry.get('waveDirection_sg'),
                entry.get('wavePeriod_sg'),
                entry.get('swellHeight_sg'),
                entry.get('swellDirection_sg'),
                entry.get('swellPeriod_sg'),
                entry.get('secondarySwellHeight_sg'),
                entry.get('secondarySwellDirection_sg'),
                entry.get('secondarySwellPeriod_sg'),
                entry.get('windSpeed_sg'),
                entry.get('windDirection_sg'),
                entry.get('waterTemperature_sg'),
                entry.get('airTemperature_sg'),
                entry.get('currentSpeed_sg'),
                entry.get('currentDirection_sg'),
                entry.get('seaLevel_sg')
            )
            try:
                await conn.execute(
                    """
                    INSERT INTO forecasts (
                        spot_id, timestamp_utc, wave_height_sg, wave_direction_sg, wave_period_sg,
                        swell_height_sg, swell_direction_sg, swell_period_sg, secondary_swell_height_sg,
                        secondary_swell_direction_sg, secondary_swell_period_sg, wind_speed_sg,
                        wind_direction_sg, water_temperature_sg, air_temperature_sg, current_speed_sg,
                        current_direction_sg, sea_level_sg
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
                    )
                    ON CONFLICT (spot_id, timestamp_utc) DO UPDATE SET
                        wave_height_sg = EXCLUDED.wave_height_sg,
                        wave_direction_sg = EXCLUDED.wave_direction_sg,
                        wave_period_sg = EXCLUDED.wave_period_sg,
                        swell_height_sg = EXCLUDED.swell_height_sg,
                        swell_direction_sg = EXCLUDED.swell_direction_sg,
                        swell_period_sg = EXCLUDED.swell_period_sg,
                        secondary_swell_height_sg = EXCLUDED.secondary_swell_height_sg,
                        secondary_swell_direction_sg = EXCLUDED.secondary_swell_direction_sg,
                        secondary_swell_period_sg = EXCLUDED.secondary_swell_period_sg,
                        wind_speed_sg = EXCLUDED.wind_speed_sg,
                        wind_direction_sg = EXCLUDED.wind_direction_sg,
                        water_temperature_sg = EXCLUDED.water_temperature_sg,
                        air_temperature_sg = EXCLUDED.air_temperature_sg,
                        current_speed_sg = EXCLUDED.current_speed_sg,
                        current_direction_sg = EXCLUDED.current_direction_sg,
                        sea_level_sg = EXCLUDED.sea_level_sg;
                    """,
                    *values_to_insert
                )
            except Exception as e:
                print(f"Error inserting/updating forecast for {spot_id} at {timestamp_utc}: {e}")
    finally:
        await release_async_db_connection(conn)
    print("Forecast insertion/update process finished.")

async def insert_extreme_tides_data(spot_id, extremes_data):
    """
    Inserts/Updates the tide extremes data into the tides_forecast table.
    Usa context manager para conexão e cursor.
    """
    if not extremes_data:
        print("No tide extremes data to insert.")
        return

    print(f"Starting insertion/update of {len(extremes_data)} tide extremes...")
    conn = await get_async_db_connection()
    try:
        for extreme in extremes_data:
            timestamp_utc = datetime.datetime.fromisoformat(extreme['time'].replace('Z', '+00:00')).replace(tzinfo=datetime.timezone.utc)
            tide_type = extreme['type']
            tide_height = extreme.get('height')
            try:
                await conn.execute(
                    """
                    INSERT INTO tides_forecast (spot_id, timestamp_utc, tide_type, height)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (spot_id, timestamp_utc, tide_type) DO UPDATE SET
                        tide_type = EXCLUDED.tide_type,
                        height = EXCLUDED.height;
                    """,
                    spot_id, timestamp_utc, tide_type, tide_height
                )
            except Exception as e:
                print(f"Error inserting/updating tide extreme for {spot_id} at {timestamp_utc}: {e}")
    finally:
        await release_async_db_connection(conn)
    print("Tide extremes insertion/update process finished.")

    # --- Funções de Leitura de Dados (GET) ---

async def get_all_spots():
    """
    Recupera todos os spots de surf do banco de dados.
    Retorna uma lista de dicionários, cada um representando um spot, com chaves em snake_case.
    """
    conn = await get_async_db_connection()
    try:
        rows = await conn.fetch("SELECT spot_id, spot_name, latitude, longitude, timezone FROM spots ORDER BY spot_id;")
        if not rows:
            print("No spots found in the database. Please add spots.")
            return []
        return [dict(row) for row in rows]
    finally:
        await release_async_db_connection(conn)

async def get_spot_by_id(spot_id):
    """
    Fetches details for a single surf spot by its ID.
    Returns a dictionary with keys in snake_case.
    """
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow("SELECT spot_id, spot_name, latitude, longitude, timezone FROM spots WHERE spot_id = $1;", spot_id)
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

async def get_forecasts_from_db(spot_id, start_utc, end_utc):
    """
    Fetches forecast data for a specific spot within a given UTC time range.
    Returns a list of dictionaries with forecast entries.
    """
    conn = await get_async_db_connection()
    try:
        rows = await conn.fetch(
            """
            SELECT
                timestamp_utc, wave_height_sg, wave_direction_sg, wave_period_sg,
                swell_height_sg, swell_direction_sg, swell_period_sg,
                secondary_swell_height_sg, secondary_swell_direction_sg, secondary_swell_period_sg,
                wind_speed_sg, wind_direction_sg, water_temperature_sg, air_temperature_sg,
                current_speed_sg, current_direction_sg, sea_level_sg
            FROM forecasts
            WHERE spot_id = $1 AND timestamp_utc BETWEEN $2 AND $3
            ORDER BY timestamp_utc;
            """,
            spot_id, start_utc, end_utc
        )
        return [dict(row) for row in rows]
    finally:
        await release_async_db_connection(conn)

async def get_tides_forecast_from_db(spot_id, start_utc, end_utc):
    """
    Fetches tide forecast data for a specific spot within a given UTC time range.
    Returns a list of dictionaries with tide entries.
    """
    conn = await get_async_db_connection()
    try:
        rows = await conn.fetch(
            """
            SELECT
                timestamp_utc, tide_type, height
            FROM tides_forecast
            WHERE spot_id = $1 AND timestamp_utc BETWEEN $2 AND $3
            ORDER BY timestamp_utc;
            """,
            spot_id, start_utc, end_utc
        )
        return [dict(row) for row in rows]
    finally:
        await release_async_db_connection(conn)

# --- Funções de Usuário ---

async def create_user(name, email, password_hash, surf_level, goofy_regular_stance,
                preferred_wave_direction, bio, profile_picture_url):
    conn = await get_async_db_connection()
    try:
        user_id = await conn.fetchval(
            """
            INSERT INTO users (name, email, password_hash, surf_level, goofy_regular_stance,
                                preferred_wave_direction, bio, profile_picture_url)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING user_id;
            """,
            name, email, password_hash, surf_level, goofy_regular_stance,
            preferred_wave_direction, bio, profile_picture_url
        )
        return user_id
    finally:
        await release_async_db_connection(conn)

async def get_user_by_email(email):
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow("SELECT * FROM users WHERE email = $1;", email)
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

async def get_user_by_id(user_id):
    """
    Fetches user data by user_id.
    Returns a dictionary with keys in snake_case.
    """
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow(
            "SELECT user_id, name, email, password_hash, surf_level, goofy_regular_stance, preferred_wave_direction, bio, profile_picture_url, registration_timestamp, last_login_timestamp FROM users WHERE user_id = $1;",
            user_id
        )
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

async def update_user_last_login(user_id):
    conn = await get_async_db_connection()
    try:
        await conn.execute(
            """
            UPDATE users
            SET last_login_timestamp = NOW()
            WHERE user_id = $1;
            """,
            str(user_id)
        )
    finally:
        await release_async_db_connection(conn)

async def update_user_profile(user_id, updates: dict):
    if not updates:
        return
    conn = await get_async_db_connection()
    try:
        query_parts = []
        values_for_query = []
        for key, value in updates.items():
            query_parts.append(f"{key} = ${len(values_for_query)+1}")
            values_for_query.append(value)
        query_sql = f"UPDATE users SET {', '.join(query_parts)} WHERE user_id = ${len(values_for_query)+1};"
        values_for_query.append(str(user_id))
        await conn.execute(query_sql, *values_for_query)
    finally:
        await release_async_db_connection(conn)

async def get_user_surf_level(user_id):
    """
    Recupera o nível de surf de um usuário pelo seu ID.
    Retorna o nível de surf como string ou None se não encontrado.
    """
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow("SELECT surf_level FROM users WHERE user_id = $1;", str(user_id))
        return row['surf_level'] if row else None
    finally:
        await release_async_db_connection(conn)

async def get_spot_preferences(user_id, spot_id, preference_type='model'):
    """
    Recupera as preferências de um spot para um usuário,
    podendo ser do modelo ('model') ou manual ('user').
    Retorna um dicionário com as preferências ou None.
    """
    if preference_type == 'model':
        table_name = "model_spot_preferences"
    elif preference_type == 'user':
        table_name = "user_spot_preferences"
    else:
        raise ValueError("preference_type deve ser 'model' ou 'user'.")
    conn = await get_async_db_connection()
    try:
        # Adicionar condição is_active = TRUE apenas para user_spot_preferences
        if preference_type == 'user':
            row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE user_id = $1 AND spot_id = $2 AND is_active = TRUE;", str(user_id), spot_id)
        else: # Para model_spot_preferences, não há is_active ou ele é sempre TRUE
            row = await conn.fetchrow(f"SELECT * FROM {table_name} WHERE user_id = $1 AND spot_id = $2;", str(user_id), spot_id)
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

async def get_level_spot_preferences(surf_level, spot_id):
    """
    Recupera as preferências de um spot para um nível de surf específico.
    Retorna um dicionário com as preferências ou None.
    """
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM level_spot_preferences WHERE surf_level = $1 AND spot_id = $2;",
            surf_level, spot_id
        )
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

# --- Funções para user_recommendation_presets ---

async def create_user_recommendation_preset(user_id, preset_name, spot_ids, start_time, end_time, weekdays=None, is_default=False):
    """
    Cria um novo preset de recomendação para um usuário.
    Agora usa 'weekdays'.
    """
    conn = await get_async_db_connection()
    try:
        if is_default:
            await conn.execute("UPDATE user_recommendation_presets SET is_default = FALSE WHERE user_id = $1 AND is_default = TRUE;", str(user_id))
        
        # Garante que weekdays seja uma lista, mesmo que vazia, se for None
        weekdays_value = weekdays if weekdays is not None else []
            
        preset_id = await conn.fetchval(
            """
            INSERT INTO user_recommendation_presets (user_id, preset_name, spot_ids, start_time, end_time, weekdays, is_default)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING preset_id;
            """,
            str(user_id), preset_name, spot_ids, start_time, end_time, weekdays_value, is_default
        )
        return preset_id
    finally:
        await release_async_db_connection(conn)

async def get_user_recommendation_presets(user_id):
    """
    Recupera todos os presets de recomendação de um usuário.
    Retorna uma lista de dicionários.
    """
    conn = await get_async_db_connection()
    try:
        rows = await conn.fetch("SELECT * FROM user_recommendation_presets WHERE user_id = $1 AND is_active = TRUE ORDER BY preset_name;", str(user_id))
        return [dict(row) for row in rows]
    finally:
        await release_async_db_connection(conn)

async def get_default_user_recommendation_preset(user_id):
    """
    Recupera o preset de recomendação padrão (is_default = TRUE) de um usuário.
    Retorna um dicionário ou None.
    """
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow("SELECT * FROM user_recommendation_presets WHERE user_id = $1 AND is_default = TRUE AND is_active = TRUE;", str(user_id))
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

async def get_user_recommendation_preset_by_id(preset_id, user_id):
    """
    Recupera um preset de recomendação específico pelo ID e user_id.
    Retorna um dicionário ou None.
    """
    conn = await get_async_db_connection()
    try:
        row = await conn.fetchrow("SELECT * FROM user_recommendation_presets WHERE preset_id = $1 AND user_id = $2 AND is_active = TRUE;", preset_id, str(user_id))
        return dict(row) if row else None
    finally:
        await release_async_db_connection(conn)

async def update_user_recommendation_preset(preset_id, user_id, updates: dict):
    """
    Atualiza um preset de recomendação existente.
    """
    if not updates:
        return False
    
    conn = await get_async_db_connection()
    try:
        query_parts = []
        values_for_query = []
        
        if 'is_default' in updates and updates['is_default']:
            await conn.execute("UPDATE user_recommendation_presets SET is_default = FALSE WHERE user_id = $1 AND is_default = TRUE AND preset_id != $2;", str(user_id), preset_id)
            
        for key, value in updates.items():
            query_parts.append(f"{key} = ${len(values_for_query) + 1}")
            
            values_for_query.append(value)
            
        query_sql = f"UPDATE user_recommendation_presets SET {', '.join(query_parts)}, updated_at = NOW() WHERE preset_id = ${len(values_for_query) + 1} AND user_id = ${len(values_for_query) + 2};"
        values_for_query.append(preset_id)
        values_for_query.append(str(user_id))
        
        result = await conn.execute(query_sql, *values_for_query)
        return result[-1] != '0'  # rowcount > 0
        
    finally:
        await release_async_db_connection(conn)


async def delete_user_recommendation_preset(preset_id, user_id):
    """
    "Soft-deleta" um preset de recomendação, marcando-o como inativo.
    """
    conn = await get_async_db_connection()
    try:
        result = await conn.execute(
            """
            UPDATE user_recommendation_presets
            SET is_active = FALSE, updated_at = NOW()
            WHERE preset_id = $1 AND user_id = $2;
            """,
            preset_id, str(user_id)
        )
        return result[-1] != '0'  # rowcount > 0
    finally:
        await release_async_db_connection(conn)

async def set_user_spot_preferences(user_id, spot_id, preferences: dict):
    # ON CONFLICT lida com inserções e atualizações.
    conn = await get_async_db_connection()
    try:
        # As chaves em 'preferences' devem corresponder aos nomes das colunas
        columns = ", ".join(preferences.keys())
        placeholders = ", ".join([f"${i+3}" for i in range(len(preferences))])
        update_setters = ", ".join([f"{key} = EXCLUDED.{key}" for key in preferences.keys()])

        query = f"""
        INSERT INTO user_spot_preferences (user_id, spot_id, {columns})
        VALUES ($1, $2, {placeholders})
        ON CONFLICT (user_id, spot_id) DO UPDATE SET
        {update_setters};
        """
        await conn.execute(query, str(user_id), spot_id, *preferences.values())
    finally:
        await release_async_db_connection(conn)

async def toggle_spot_preference_active(user_id, spot_id, is_active: bool):
    conn = await get_async_db_connection()
    try:
        # Atualiza a coluna 'is_active' na tabela user_spot_preferences
        await conn.execute(
            """
            UPDATE user_spot_preferences
            SET is_active = $1
            WHERE user_id = $2 AND spot_id = $3;
            """,
            is_active, str(user_id), spot_id
        )
    finally:
        await release_async_db_connection(conn)


# --- Sugestão de índice para performance ---
# Certifique-se de ter índices em:
# - spots(spot_name)
# - forecasts(spot_id, timestamp_utc)
# - tides_forecast(spot_id, timestamp_utc)
# - users(email)
# - user_recommendation_presets(user_id, is_active)
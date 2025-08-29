
import os
import datetime
from src.db.connection import get_async_db_connection, release_async_db_connection

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
                entry.get('seaLevel_sg'),
                entry.get('tide_type') # Novo campo
            )
            try:
                await conn.execute(
                    """
                    INSERT INTO forecasts (
                        spot_id, timestamp_utc, wave_height_sg, wave_direction_sg, wave_period_sg,
                        swell_height_sg, swell_direction_sg, swell_period_sg, secondary_swell_height_sg,
                        secondary_swell_direction_sg, secondary_swell_period_sg, wind_speed_sg,
                        wind_direction_sg, water_temperature_sg, air_temperature_sg, current_speed_sg,
                        current_direction_sg, sea_level_sg, tide_type
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
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
                        sea_level_sg = EXCLUDED.sea_level_sg,
                        tide_type = EXCLUDED.tide_type;
                    """,
                    *values_to_insert
                )
            except Exception as e:
                print(f"Error inserting/updating forecast for {spot_id} at {timestamp_utc}: {e}")
    finally:
        await release_async_db_connection(conn)
    print("Forecast insertion/update process finished.")
    
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

async def delete_old_forecast_data(days_to_keep=7):
    """
    Deletes forecast data older than a specified number of days from the database.
    """
    conn = await get_async_db_connection()
    try:
        # Define o intervalo de tempo
        time_threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_to_keep)
        
        print(f"\nIniciando limpeza de dados de previsão anteriores a {days_to_keep} dias...")

        # Deleta da tabela forecasts
        result_forecasts = await conn.execute(
            "DELETE FROM forecasts WHERE timestamp_utc < $1",
            time_threshold
        )
        # O resultado do execute retorna uma string como 'DELETE 250'
        deleted_count_forecasts = int(result_forecasts.split(' ')[1])
        print(f"{deleted_count_forecasts} registros antigos removidos da tabela 'forecasts'.")

        # Deleta da tabela tides_forecast
        result_tides = await conn.execute(
            "DELETE FROM tides_forecast WHERE timestamp_utc < $1",
            time_threshold
        )
        deleted_count_tides = int(result_tides.split(' ')[1])
        print(f"{deleted_count_tides} registros antigos removidos da tabela 'tides_forecast'.")

    except Exception as e:
        print(f"Erro durante a limpeza de dados antigos: {e}")
    finally:
        await release_async_db_connection(conn)
    print("Limpeza de dados antigos finalizada.")
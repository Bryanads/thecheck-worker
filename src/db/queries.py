import datetime
from src.db.connection import get_async_db_connection, release_async_db_connection

async def insert_forecast_data(spot_id, forecast_data):
    """
    Insere/Atualiza os dados de previsão na tabela `forecasts`.
    Adaptado para o schema V2.
    """
    if not forecast_data:
        print("Nenhum dado horário para inserir.")
        return

    print(f"Iniciando inserção/atualização de {len(forecast_data)} previsões horárias...")
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
                entry.get('tide_type')
            )
            try:
                # Query ajustada para o novo schema da tabela 'forecasts'
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
                        current_direction_sg = EXcluded.current_direction_sg,
                        sea_level_sg = EXCLUDED.sea_level_sg,
                        tide_type = EXCLUDED.tide_type;
                    """,
                    *values_to_insert
                )
            except Exception as e:
                print(f"Erro ao inserir/atualizar previsão para {spot_id} em {timestamp_utc}: {e}")
    finally:
        await release_async_db_connection(conn)
    print("Processo de inserção/atualização de previsões finalizado.")

async def get_all_spots():
    """
    Recupera todos os spots de surf do banco de dados para o worker.
    """
    conn = await get_async_db_connection()
    try:
        # Query ajustada para buscar as novas colunas
        rows = await conn.fetch("SELECT spot_id, name as spot_name, latitude, longitude, timezone FROM spots ORDER BY spot_id;")
        if not rows:
            print("Nenhum spot encontrado no banco de dados. Por favor, adicione spots.")
            return []
        return [dict(row) for row in rows]
    finally:
        await release_async_db_connection(conn)

async def delete_old_forecast_data(days_to_keep=7):
    """
    Deleta dados de previsão mais antigos que um número especificado de dias.
    """
    conn = await get_async_db_connection()
    try:
        time_threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_to_keep)
        
        print(f"\nIniciando limpeza de dados de previsão anteriores a {time_threshold.strftime('%Y-%m-%d')}...")

        result_forecasts = await conn.execute(
            "DELETE FROM forecasts WHERE timestamp_utc < $1",
            time_threshold
        )
        deleted_count = int(result_forecasts.split(' ')[1]) if 'DELETE' in result_forecasts else 0
        print(f"{deleted_count} registros antigos removidos da tabela 'forecasts'.")

    except Exception as e:
        print(f"Erro durante a limpeza de dados antigos: {e}")
    finally:
        await release_async_db_connection(conn)
    print("Limpeza de dados antigos finalizada.")
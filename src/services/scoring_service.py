import numpy as np
from typing import Dict, Any

# --- Lógica do Score de Onda (Baseado em wave_score.py) ---
def _calculate_swell_size_score(swell_height: float, ideal_height: float, max_height: float) -> float:
    if swell_height > max_height:
        return -100.0
    if swell_height < (ideal_height * 0.3):
        return 0.0

    if swell_height <= ideal_height:
        return 100 * (swell_height / ideal_height)
    else:
        range_size = max_height - ideal_height
        if range_size <= 0: return 0.0
        return 100 * (1 - (swell_height - ideal_height) / range_size)

def _calculate_swell_period_score(swell_period: float, surf_level: str) -> float:
    # *** DICIONÁRIO ATUALIZADO AQUI ***
    ideal_periods = {
        'iniciante': 8,
        'maroleiro': 10,  # Maroleiro gosta de onda mais em pé, com mais linha
        'intermediario': 12,
        'pro': 15         # Pro busca o máximo de power
    }
    ideal_period = ideal_periods.get(surf_level, 12) # Padrão para intermediário
    score = np.exp(-((swell_period - ideal_period) ** 2) / ideal_period) * 100
    return score

def _calculate_swell_direction_score(swell_direction: float, ideal_directions: list) -> float:
    if not ideal_directions: return 50.0 # Neutro se não houver direção ideal

    # Convertendo a lista de ideais para float para evitar TypeError
    ideal_directions = [float(d) for d in ideal_directions]

    min_diff = 360
    for ideal_dir in ideal_directions:
        diff = abs(swell_direction - ideal_dir)
        min_diff = min(min_diff, diff, 360 - diff)

    score = np.exp(-(min_diff**2) / (45**2)) * 100
    return score

def _calculate_wave_score(forecast: Dict, prefs: Dict, spot: Dict, profile: Dict) -> float:
    swell_height = float(forecast.get('swell_height_sg', 0))
    swell_period = float(forecast.get('swell_period_sg', 0))
    swell_direction = float(forecast.get('swell_direction_sg', 0))

    # Sub-scores
    size_score = _calculate_swell_size_score(
        swell_height,
        float(prefs.get('ideal_swell_height', 1.5)),
        float(prefs.get('max_swell_height', 2.5))
    )
    if size_score < 0: return 0.0 # Se for muito grande, a nota da onda é zero.

    period_score = _calculate_swell_period_score(swell_period, profile.get('surf_level', 'intermediario'))
    direction_score = _calculate_swell_direction_score(swell_direction, spot.get('ideal_swell_direction', []))

    # Score Base
    score_base = (size_score * 0.70) + (period_score * 0.15) + (direction_score * 0.15)

    # TODO: Implementar penalidades de inconsistência e swell secundário
    return round(np.clip(score_base, 0, 100), 2)


# --- Lógica do Score de Vento (Baseado em wind_score.py) ---
def _calculate_wind_score(forecast: Dict, prefs: Dict, spot: Dict) -> float:
    wind_speed = float(forecast.get('wind_speed_sg', 0))
    wind_dir = float(forecast.get('wind_direction_sg', 0))
    max_wind = float(prefs.get('max_wind_speed', 8.0))
    ideal_dirs = spot.get('ideal_wind_direction', [])

    # Convertendo a lista de ideais para float para evitar TypeError
    ideal_dirs = [float(d) for d in ideal_dirs]

    if wind_speed > max_wind:
        return 0.0

    if not ideal_dirs: return 75.0 # Neutro se não houver direção ideal

    min_diff = 360
    for ideal_dir in ideal_dirs:
        diff = abs(wind_dir - ideal_dir)
        min_diff = min(min_diff, diff, 360 - diff)

    if min_diff <= 45: # Terral
        # Lógica simplificada: terral é bom, mas vento forte é ruim
        return 100 * (1 - (wind_speed / max_wind))
    else: # Maral/Lateral
        # Penalidade maior para maral
        return 75 * (1 - (wind_speed / max_wind))


# --- Lógica do Score de Maré (Baseado em tide_score.py) ---
def _calculate_tide_score(forecast: Dict, spot: Dict) -> float:
    sea_level = float(forecast.get('sea_level_sg', 0))
    tide_type = forecast.get('tide_type', '')
    ideal_level = float(spot.get('ideal_sea_level', 0.5))
    ideal_flow = spot.get('ideal_tide_flow', [])

    # Score da Altura (curva de sino)
    score_altura = np.exp(-((sea_level - ideal_level) ** 2) / 0.5) * 100

    # Penalidade pelo fluxo
    if ideal_flow and tide_type not in ideal_flow:
        score_altura *= 0.8

    return round(score_altura, 2)

# --- Lógica do Score de Temperatura (Baseado em temperature_score.py) ---
def _calculate_air_temperature_score(forecast: Dict, prefs: Dict) -> float:
    air_temp = float(forecast.get('air_temperature_sg', 25))
    ideal_air = float(prefs.get('ideal_air_temperature', 25))

    score_ar = np.exp(-0.04 * ((air_temp - ideal_air) ** 2)) * 100

    return round(score_ar, 2)

def _calculate_water_temperature_score(forecast: Dict, prefs: Dict) -> float:
    water_temp = float(forecast.get('water_temperature_sg', 22))
    ideal_water = float(prefs.get('ideal_water_temperature', 22))

    score_agua = np.exp(-0.08 * ((water_temp - ideal_water) ** 2)) * 100
    return round(score_agua,2)


# --- Função Principal ---
async def calculate_overall_score(forecast: Dict, prefs: Dict, spot: Dict, profile: Dict) -> dict:
    """
    Calcula o score geral e os scores detalhados para uma única hora de previsão.
    """
    wave_score = _calculate_wave_score(forecast, prefs, spot, profile)
    wind_score = _calculate_wind_score(forecast, prefs, spot)
    tide_score = _calculate_tide_score(forecast, spot)
    water_temperature_score = _calculate_water_temperature_score(forecast, prefs)
    air_temperature_score = _calculate_air_temperature_score(forecast, prefs)

    # Média Ponderada Final
    overall_score = (
        (wave_score * 0.50) +
        (wind_score * 0.33) +
        (tide_score * 0.15) +
        (air_temperature_score * 0.01) +
        (water_temperature_score * 0.01)
    )

    return {
        "overall_score": round(overall_score, 2),
        "detailed_scores": {
            "wave_score": wave_score,
            "wind_score": wind_score,
            "tide_score": tide_score,
            "air_temperature_score": air_temperature_score,
            "water_temperature_score": water_temperature_score,
        }
    }
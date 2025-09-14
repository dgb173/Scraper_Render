# app.py - Servidor web principal (Flask)
from flask import Flask, render_template, abort, request
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import datetime
import re
import math

# ¡Importante! Importa tu nuevo módulo de scraping
from modules.estudio_scraper import obtener_datos_completos_partido, format_ah_as_decimal_string_of, obtener_datos_preview_rapido, obtener_datos_preview_ligero
from flask import jsonify # Asegúrate de que jsonify está importado

app = Flask(__name__)

# --- Mantén tu lógica para la página principal ---
URL_NOWGOAL = "https://live20.nowgoal25.com/"

def _parse_number_clean(s: str):
    if s is None:
        return None
    txt = str(s).strip()
    txt = txt.replace('\u2212', '-')  # unicode minus
    txt = txt.replace(',', '.')
    txt = txt.replace('+', '')
    txt = txt.replace(' ', '')
    m = re.search(r"^[+-]?\d+(?:\.\d+)?$", txt)
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return None
    return None

def _parse_number(s: str):
    if s is None:
        return None
    # Normaliza separadores y signos
    txt = str(s).strip()
    txt = txt.replace('\u2212', '-')  # minus unicode
    txt = txt.replace(',', '.')
    txt = txt.replace(' ', '')
    # Coincide con un número decimal con signo
    m = re.search(r"^[+-]?\d+(?:\.\d+)?$", txt)
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return None
    return None

def _parse_handicap_to_float(text: str):
    if text is None:
        return None
    t = str(text).strip()
    if '/' in t:
        parts = [p for p in re.split(r"/", t) if p]
        nums = []
        for p in parts:
            v = _parse_number_clean(p)
            if v is None:
                return None
            nums.append(v)
        if not nums:
            return None
        return sum(nums) / len(nums)
    # Si viene como cadena normal (ej. "+0.25" o "-0,75")
    return _parse_number_clean(t.replace('+', ''))

def _bucket_to_half(value: float) -> float:
    if value is None:
        return None
    if value == 0:
        return 0.0
    sign = -1.0 if value < 0 else 1.0
    av = abs(value)
    base = math.floor(av + 1e-9)
    frac = av - base
    # Mapea 0.25/0.75/0.5 a .5, 0.0 queda .0
    def close(a, b):
        return abs(a - b) < 1e-6
    if close(frac, 0.0):
        bucket = float(base)
    elif close(frac, 0.5) or close(frac, 0.25) or close(frac, 0.75):
        bucket = base + 0.5
    else:
        # fallback: redondeo al múltiplo de 0.5 más cercano
        bucket = round(av * 2) / 2.0
        # si cae justo en entero, desplazar a .5 para respetar la preferencia de .25/.75 → .5
        f = bucket - math.floor(bucket)
        if close(f, 0.0) and (abs(av - (math.floor(bucket) + 0.25)) < 0.26 or abs(av - (math.floor(bucket) + 0.75)) < 0.26):
            bucket = math.floor(bucket) + 0.5
    return sign * bucket

def normalize_handicap_to_half_bucket_str(text: str):
    v = _parse_handicap_to_float(text)
    if v is None:
        return None
    b = _bucket_to_half(v)
    if b is None:
        return None
    # Formato con un decimal
    return f"{b:.1f}"


def parse_main_page_matches(html_content, limit=20, offset=0, handicap_filter=None):
    soup = BeautifulSoup(html_content, 'html.parser')
    match_rows = soup.find_all('tr', id=lambda x: x and x.startswith('tr1_'))
    upcoming_matches = []
    now_utc = datetime.datetime.utcnow()

    for row in match_rows:
        match_id = row.get('id', '').replace('tr1_', '')
        if not match_id: continue

        time_cell = row.find('td', {'name': 'timeData'})
        if not time_cell or not time_cell.has_attr('data-t'): continue
        
        try:
            match_time = datetime.datetime.strptime(time_cell['data-t'], '%Y-%m-%d %H:%M:%S')
        except (ValueError, IndexError):
            continue

        if match_time < now_utc: continue

        home_team_tag = row.find('a', {'id': f'team1_{match_id}'})
        away_team_tag = row.find('a', {'id': f'team2_{match_id}'})
        odds_data = row.get('odds', '').split(',')
        handicap = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10] if len(odds_data) > 10 else "N/A"

        # Filtro robusto: Ignorar si no hay datos de handicap o goles
        if not handicap or handicap == "N/A" or not goal_line or goal_line == "N/A":
            continue

        upcoming_matches.append({
            "id": match_id,
            "time": match_time.strftime('%Y-%m-%d %H:%M'),
            "home_team": home_team_tag.text.strip() if home_team_tag else "N/A",
            "away_team": away_team_tag.text.strip() if away_team_tag else "N/A",
            "handicap": handicap,
            "goal_line": goal_line
        })

    # Aplicar filtro por hándicap mapeando a pasos de 0.5
    if handicap_filter:
        try:
            target = normalize_handicap_to_half_bucket_str(handicap_filter)
            if target is not None:
                filtered = []
                for m in upcoming_matches:
                    hv = normalize_handicap_to_half_bucket_str(m.get('handicap', ''))
                    if hv == target:
                        filtered.append(m)
                upcoming_matches = filtered
        except Exception:
            pass

    upcoming_matches.sort(key=lambda x: x['time'])
    return upcoming_matches[offset:offset+limit]

async def get_main_page_matches_async(limit=20, offset=0, handicap_filter=None):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(URL_NOWGOAL, wait_until="domcontentloaded", timeout=20000)
            await page.wait_for_timeout(5000)
            html_content = await page.content()
            return parse_main_page_matches(html_content, limit, offset, handicap_filter)
        finally:
            await browser.close()

@app.route('/')
def index():
    try:
        print("Recibida petición. Ejecutando scraper de partidos...")
        hf = request.args.get('handicap')
        matches = asyncio.run(get_main_page_matches_async(handicap_filter=hf))
        print(f"Scraper finalizado. {len(matches)} partidos encontrados.")
        opts = sorted({
            normalize_handicap_to_half_bucket_str(m.get('handicap'))
            for m in matches if normalize_handicap_to_half_bucket_str(m.get('handicap')) is not None
        }, key=lambda x: float(x))
        return render_template('index.html', matches=matches, handicap_filter=hf, handicap_options=opts)
    except Exception as e:
        print(f"ERROR en la ruta principal: {e}")
        return render_template('index.html', matches=[], error=f"No se pudieron cargar los partidos: {e}")

@app.route('/healthz')
def healthz():
    return 'ok', 200

@app.route('/api/matches')
def api_matches():
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 5))
        matches = asyncio.run(get_main_page_matches_async(limit, offset, request.args.get('handicap')))
        return {'matches': matches}
    except Exception as e:
        return {'error': str(e)}, 500

@app.route('/proximos')
def proximos():
    try:
        print("Recibida petición. Ejecutando scraper de partidos...")
        hf = request.args.get('handicap')
        matches = asyncio.run(get_main_page_matches_async(25, 0, hf))
        print(f"Scraper finalizado. {len(matches)} partidos encontrados.")
        opts = sorted({
            normalize_handicap_to_half_bucket_str(m.get('handicap'))
            for m in matches if normalize_handicap_to_half_bucket_str(m.get('handicap')) is not None
        }, key=lambda x: float(x))
        return render_template('index.html', matches=matches, handicap_filter=hf, handicap_options=opts)
    except Exception as e:
        print(f"ERROR en la ruta principal: {e}")
        return render_template('index.html', matches=[], error=f"No se pudieron cargar los partidos: {e}")

# --- NUEVA RUTA PARA MOSTRAR EL ESTUDIO DETALLADO ---
@app.route('/estudio/<string:match_id>')
def mostrar_estudio(match_id):
    """
    Esta ruta se activa cuando un usuario visita /estudio/ID_DEL_PARTIDO.
    """
    print(f"Recibida petición para el estudio del partido ID: {match_id}")
    
    # Llama a la función principal de tu módulo de scraping
    datos_partido = obtener_datos_completos_partido(match_id)
    
    if not datos_partido or "error" in datos_partido:
        # Si hay un error, puedes mostrar una página de error
        print(f"Error al obtener datos para {match_id}: {datos_partido.get('error')}")
        abort(500, description=datos_partido.get('error', 'Error desconocido'))

    # Si todo va bien, renderiza la plantilla HTML pasándole los datos
    print(f"Datos obtenidos para {datos_partido['home_name']} vs {datos_partido['away_name']}. Renderizando plantilla...")
    return render_template('estudio.html', data=datos_partido, format_ah=format_ah_as_decimal_string_of)

# --- NUEVA RUTA PARA ANALIZAR PARTIDOS FINALIZADOS ---
@app.route('/analizar_partido', methods=['GET', 'POST'])
def analizar_partido():
    """
    Ruta para analizar partidos finalizados por ID.
    """
    if request.method == 'POST':
        match_id = request.form.get('match_id')
        if match_id:
            print(f"Recibida petición para analizar partido finalizado ID: {match_id}")
            
            # Llama a la función principal de tu módulo de scraping
            datos_partido = obtener_datos_completos_partido(match_id)
            
            if not datos_partido or "error" in datos_partido:
                # Si hay un error, mostrarlo en la página
                print(f"Error al obtener datos para {match_id}: {datos_partido.get('error')}")
                return render_template('analizar_partido.html', error=datos_partido.get('error', 'Error desconocido'))
            
            # Si todo va bien, renderiza la plantilla HTML pasándole los datos
            print(f"Datos obtenidos para {datos_partido['home_name']} vs {datos_partido['away_name']}. Renderizando plantilla...")
            return render_template('estudio.html', data=datos_partido, format_ah=format_ah_as_decimal_string_of)
        else:
            return render_template('analizar_partido.html', error="Por favor, introduce un ID de partido válido.")
    
    # Si es GET, mostrar el formulario
    return render_template('analizar_partido.html')

# --- NUEVA RUTA API PARA LA VISTA PREVIA RÁPIDA ---
@app.route('/api/preview/<string:match_id>')
def api_preview(match_id):
    """
    Endpoint para la vista previa. Llama al scraper LIGERO y RÁPIDO.
    Devuelve los datos en formato JSON.
    """
    try:
        # Por defecto usa la vista previa LIGERA (requests). Si ?mode=selenium, usa la completa.
        mode = request.args.get('mode', 'light').lower()
        if mode in ['full', 'selenium']:
            preview_data = obtener_datos_preview_rapido(match_id)
        else:
            preview_data = obtener_datos_preview_ligero(match_id)
        if "error" in preview_data:
            return jsonify(preview_data), 500
        return jsonify(preview_data)
    except Exception as e:
        print(f"Error en la ruta /api/preview/{match_id}: {e}")
        return jsonify({'error': 'Ocurrió un error interno en el servidor.'}), 500


@app.route('/api/analisis/<string:match_id>')
def api_analisis(match_id):
    """
    Servicio de analisis profundo bajo demanda.
    Devuelve solo:
    - Rendimiento Reciente y H2H Indirecto (3 columnas)
    - Comparativas Indirectas (2 columnas)
    """
    try:
        datos = obtener_datos_completos_partido(match_id)
        if not datos or (isinstance(datos, dict) and datos.get('error')):
            return jsonify({'error': (datos or {}).get('error', 'No se pudieron obtener datos.')}), 500

        def df_to_rows(df):
            rows = []
            try:
                if df is not None and hasattr(df, 'iterrows'):
                    for idx, row in df.iterrows():
                        label = str(idx)
                        label = label.replace('Shots on Goal', 'Tiros a Puerta') \
                                     .replace('Shots', 'Tiros') \
                                     .replace('Dangerous Attacks', 'Ataques Peligrosos') \
                                     .replace('Attacks', 'Ataques')
                        try:
                            home_val = row['Casa']
                        except Exception:
                            home_val = ''
                        try:
                            away_val = row['Fuera']
                        except Exception:
                            away_val = ''
                        rows.append({'label': label, 'home': home_val or '', 'away': away_val or ''})
            except Exception:
                pass
            return rows

        payload = {
            'home_team': datos.get('home_name', ''),
            'away_team': datos.get('away_name', ''),
            'recent_indirect_full': {
                'last_home': None,
                'last_away': None,
                'h2h_col3': None
            },
            'comparativas_indirectas': {
                'left': None,
                'right': None
            }
        }

        # Ultimo del Local
        last_home = (datos.get('last_home_match') or {})
        last_home_details = last_home.get('details') or {}
        if last_home_details:
            payload['recent_indirect_full']['last_home'] = {
                'home': last_home_details.get('home_team'),
                'away': last_home_details.get('away_team'),
                'score': (last_home_details.get('score') or '').replace(':', ' : '),
                'ah': format_ah_as_decimal_string_of(last_home_details.get('handicap_line_raw') or '-'),
                'ou': last_home_details.get('ouLine') or '-',
                'stats_rows': df_to_rows(last_home.get('stats'))
            }

        # Ultimo del Visitante
        last_away = (datos.get('last_away_match') or {})
        last_away_details = last_away.get('details') or {}
        if last_away_details:
            payload['recent_indirect_full']['last_away'] = {
                'home': last_away_details.get('home_team'),
                'away': last_away_details.get('away_team'),
                'score': (last_away_details.get('score') or '').replace(':', ' : '),
                'ah': format_ah_as_decimal_string_of(last_away_details.get('handicap_line_raw') or '-'),
                'ou': last_away_details.get('ouLine') or '-',
                'stats_rows': df_to_rows(last_away.get('stats'))
            }

        # H2H Rivales (Col3)
        h2h_col3 = (datos.get('h2h_col3') or {})
        h2h_col3_details = h2h_col3.get('details') or {}
        if h2h_col3_details and h2h_col3_details.get('status') == 'found':
            payload['recent_indirect_full']['h2h_col3'] = {
                'home': h2h_col3_details.get('h2h_home_team_name'),
                'away': h2h_col3_details.get('h2h_away_team_name'),
                'score': f"{h2h_col3_details.get('goles_home')} : {h2h_col3_details.get('goles_away')}",
                'ah': format_ah_as_decimal_string_of(h2h_col3_details.get('handicap_line_raw') or '-'),
                'ou': h2h_col3_details.get('ou_result') or '-',
                'stats_rows': df_to_rows(h2h_col3.get('stats'))
            }

        # Comparativas Indirectas (Left)
        comp_left = (datos.get('comp_L_vs_UV_A') or {})
        comp_left_details = comp_left.get('details') or {}
        if comp_left_details:
            payload['comparativas_indirectas']['left'] = {
                'title_home_name': datos.get('home_name'),
                'title_away_name': datos.get('away_name'),
                'home_team': comp_left_details.get('home_team'),
                'away_team': comp_left_details.get('away_team'),
                'score': (comp_left_details.get('score') or '').replace(':', ' : '),
                'ah': format_ah_as_decimal_string_of(comp_left_details.get('ah_line') or '-') if comp_left_details.get('ah_line') else '-',
                'ou': comp_left_details.get('ou_line') or '-',
                'localia': comp_left_details.get('localia') or '',
                'stats_rows': df_to_rows(comp_left.get('stats'))
            }

        # Comparativas Indirectas (Right)
        comp_right = (datos.get('comp_V_vs_UL_H') or {})
        comp_right_details = comp_right.get('details') or {}
        if comp_right_details:
            payload['comparativas_indirectas']['right'] = {
                'title_home_name': datos.get('home_name'),
                'title_away_name': datos.get('away_name'),
                'home_team': comp_right_details.get('home_team'),
                'away_team': comp_right_details.get('away_team'),
                'score': (comp_right_details.get('score') or '').replace(':', ' : '),
                'ah': format_ah_as_decimal_string_of(comp_right_details.get('ah_line') or '-') if comp_right_details.get('ah_line') else '-',
                'ou': comp_right_details.get('ou_line') or '-',
                'localia': comp_right_details.get('localia') or '',
                'stats_rows': df_to_rows(comp_right.get('stats'))
            }

        return jsonify(payload)
    except Exception as e:
        print(f"Error en la ruta /api/analisis/{match_id}: {e}")
        return jsonify({'error': 'Ocurri�� un error interno en el servidor.'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True) # debug=True es útil para desarrollar


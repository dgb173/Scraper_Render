# scraper_con_selenium.py
import datetime
import time
import sys
import pytz
from bs4 import BeautifulSoup

# Establecer la codificación de la consola a UTF-8
sys.stdout.reconfigure(encoding='utf-8')

# Importaciones de Selenium (al estilo de estudio.py)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# --- CONFIGURACIÓN (Inspirada en estudio.py) ---
URL = "https://live20.nowgoal25.com/"
SELENIUM_TIMEOUT_SECONDS = 15
# Zona horaria de Madrid
MADRID_TZ = pytz.timezone('Europe/Madrid')

# --- FUNCIÓN DE PARSEO (Idéntica a la tuya, es el método de extracción correcto) ---
def parse_match_data_from_html(html_content):
    """
    Esta función es la misma que la de tu script scraper_partidos.py.
    Utiliza BeautifulSoup para extraer los datos, lo cual es un método
    compartido y correcto en ambos de tus archivos originales.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    match_rows = soup.find_all('tr', id=lambda x: x and x.startswith('tr1_'))

    upcoming_matches = []
    now_utc = datetime.datetime.utcnow()

    for row in match_rows:
        match_id = row.get('id', '').replace('tr1_', '')
        if not match_id:
            continue

        time_cell = row.find('td', {'name': 'timeData'})
        if not time_cell or not time_cell.has_attr('data-t'):
            continue
        
        try:
            match_time_str = time_cell['data-t']
            # Parsear la hora como UTC
            match_time_utc = datetime.datetime.strptime(match_time_str, '%Y-%m-%d %H:%M:%S')
            # Hacerla timezone aware (UTC)
            match_time_utc = pytz.utc.localize(match_time_utc)
            # Convertir a hora de Madrid
            match_time_madrid = match_time_utc.astimezone(MADRID_TZ)
        except (ValueError, IndexError):
            continue

        # Verificar si el partido es en el futuro
        now_madrid = datetime.datetime.now(MADRID_TZ)
        if match_time_madrid < now_madrid:
            continue

        # Formatear las horas para mostrarlas (sin segundos)
        match_time_utc_formatted = match_time_utc.strftime('%Y-%m-%d %H:%M')
        match_time_madrid_formatted = match_time_madrid.strftime('%Y-%m-%d %H:%M')
        
        home_team_tag = row.find('a', {'id': f'team1_{match_id}'})
        home_team_name = home_team_tag.text.strip() if home_team_tag else "N/A"

        away_team_tag = row.find('a', {'id': f'team2_{match_id}'})
        away_team_name = away_team_tag.text.strip() if away_team_tag else "N/A"
        
        # Extraer cuotas de handicap y goles
        odds_data = row.get('odds', '').split(',')
        handicap = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10] if len(odds_data) > 10 else "N/A"

        if not handicap or handicap == "N/A" or not goal_line or goal_line == "N/A":
            continue

        # Extraer el nombre de la competición
        league_cell = row.find('td', {'name': 'leagueData'})
        league_name = league_cell.text.strip() if league_cell else "N/A"

        upcoming_matches.append({
            "id": match_id,
            "time_utc": match_time_utc_formatted,
            "time_madrid": match_time_madrid_formatted,
            "home_team": home_team_name,
            "away_team": away_team_name,
            "handicap": handicap,
            "goal_line": goal_line,
            "league": league_name
        })

    upcoming_matches.sort(key=lambda x: x['time_utc'])
    return upcoming_matches[:20]

# --- FUNCIÓN PARA CONFIGURAR EL DRIVER DE SELENIUM (Método de estudio.py) ---
def setup_driver():
    """
    Crea y configura una instancia del driver de Selenium,
    similar a la función get_selenium_driver_of en estudio.py.
    """
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/116.0.0.0 Safari/537.36")
    options.add_argument('--blink-settings=imagesEnabled=false')
    
    try:
        driver = webdriver.Chrome(options=options)
        return driver
    except WebDriverException as e:
        print(f"Error inicializando Selenium driver: {e}")
        return None

# --- FUNCIÓN PRINCIPAL (Lógica síncrona, como en estudio.py) ---
def main():
    print("Iniciando el scraper con el método de estudio.py (Selenium)...")
    driver = setup_driver()
    if not driver:
        print("No se pudo iniciar el navegador. Fin del script.")
        return

    try:
        print(f"Navegando a {URL}...")
        driver.get(URL)
        
        print("Página cargada. Esperando a que los datos de los partidos se carguen...")
        # Usar una espera más flexible, esperando a que haya múltiples elementos tr1_
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr[id^='tr1_']"))
        )
        # Pequeña pausa adicional para asegurar que los scripts de la página terminen de renderizar datos
        time.sleep(5)

        # Se obtiene el HTML de la página con el método de Selenium
        html_content = driver.page_source
        
        # Se llama a la misma función de parseo para extraer los datos
        next_20_matches = parse_match_data_from_html(html_content)

        print(f"\nSe encontraron {len(next_20_matches)} partidos próximos.")
        print("\n--- PRÓXIMOS 20 PARTIDOS ENCONTRADOS ---\n")
        if not next_20_matches:
            print("No se encontraron próximos partidos.")
        else:
            # Definir algunos colores ANSI para las competiciones
            colors = [
                '\033[91m',  # Rojo
                '\033[92m',  # Verde
                '\033[93m',  # Amarillo
                '\033[94m',  # Azul
                '\033[95m',  # Magenta
                '\033[96m',  # Cian
                '\033[97m',  # Blanco
            ]
            reset_color = '\033[0m'
            
            # Crear un diccionario para asignar colores a las competiciones
            league_colors = {}
            color_index = 0
            
            for match in next_20_matches:
                # Asignar un color a la competición si no tiene uno
                if match['league'] not in league_colors:
                    league_colors[match['league']] = colors[color_index % len(colors)]
                    color_index += 1
                
                # Obtener el color para esta competición
                league_color = league_colors[match['league']]
                
                # Imprimir el partido con el color de la competición
                print(f"ID: {match['id']}, Hora UTC: {match['time_utc']}, Hora Madrid: {match['time_madrid']}, {match['home_team']} vs {match['away_team']}, Handicap: {match['handicap']}, Goles: {match['goal_line']}, Competición: {league_color}{match['league']}{reset_color}")
        
        print("\n--- FIN DE LA EXTRACCIÓN ---")

    except TimeoutException:
        print(f"Tiempo de espera agotado ({SELENIUM_TIMEOUT_SECONDS}s) esperando el contenido de la página.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
    finally:
        # Se asegura que el navegador se cierre, como buena práctica
        driver.quit()
        print("Navegador cerrado. Fin del script.")

if __name__ == "__main__":
    main()
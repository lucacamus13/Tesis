# -*- coding: utf-8 -*-
"""
Script de Construcción de Base de Datos para Tesis
==================================================

Instrucciones de Uso en Google Colab:
-------------------------------------
1. Sube este archivo (`build_dataset.py`) y `requirements.txt` a tu repositorio GitHub.
2. En Colab, ejecuta:
   ```python
   !git clone https://github.com/lucacamus13/Tesis.git
   !pip install -r Tesis/requirements.txt
   !python Tesis/build_dataset.py
   ```
3. El archivo final `Thesis_Master_Dataset.xlsx` se generará en la carpeta.

Dependencias:
- pandas
- yfinance
- fredapi
- openpyxl
- requests
"""

import pandas as pd
import yfinance as yf
from fredapi import Fred
import shutil
import os
import requests
import sys

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================

# API KEY de FRED (Idealmente usar os.environ.get('FRED_API_KEY'))
FRED_API_KEY = '73be39590b688a9ca91a02451eed4cc5' 

# URLs de GitHub Raw para los archivos Excel
# Usamos raw.githubusercontent.com para asegurar descarga directa
repo_user = "lucacamus13"
repo_name = "Tesis"
branch = "main"
base_url = f"https://raw.githubusercontent.com/{repo_user}/{repo_name}/{branch}/data"

# Archivos esperados (Nombres exactos en el repo)
FILES = {
    "FF4": "USMPD-FF4 EEUU.xlsx",
    "EMBI": "Spread EMBI - ARG,CHI,MEX,BRA.xlsx",
    "ACT_BRA": "ACTIVIDAD-BRASIL.xlsx",
    "ACT_CHL": "ACTIVIDAD-CHILE.xlsx",
    "ACT_MEX": "ACTIVIDAD-MEXICO.xlsx"
}

# ==========================================
# 2. FUNCIONES UTILITARIAS
# ==========================================

def download_github_file(filename):
    """Descarga un archivo desde GitHub Raw."""
    # Reemplazar espacios por %20 para la URL
    safe_name = filename.replace(' ', '%20')
    url = f"{base_url}/{safe_name}"
    
    print(f"⬇️ Descargando: {filename} ...")
    try:
        r = requests.get(url)
        if r.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(r.content)
            print("   ✅ OK")
            return True
        else:
            print(f"   ❌ Error {r.status_code} descargando {url}")
            return False
    except Exception as e:
        print(f"   ❌ Excepción: {e}")
        return False

def clean_and_format(df, name="Series"):
    """
    Estandariza un DataFrame a frecuencia mensual (Inicio de Mes).
    Busca columnas de fecha automáticamente.
    """
    # 1. Reset index para buscar columna fecha si es necesario
    df = df.reset_index()
    
    # 2. Buscar columna de fecha
    date_col = None
    for col in df.columns:
        s_col = str(col).lower()
        if 'date' in s_col or 'fecha' in s_col or 'time' in s_col:
            date_col = col
            break
            
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.set_index(date_col)
    else:
        # Si no encontramos columna, asumimos que la primera columna es fecha o el indice ya lo era
        # Intentamos convertir el indice
        try:
            df.index = pd.to_datetime(df.index, errors='coerce')
        except:
            print(f"   ⚠️ ADVERTENCIA: No se pudo detectar fecha en {name}")
            return pd.DataFrame()

    # 3. Eliminar filas sin fecha válida
    df = df[df.index.notnull()]
    
    if df.empty:
        return df

    # 4. Resample a Mensual (Month Start) tomando el PRIMER valor del mes
    # (Ajustar a 'last' o 'mean' según la naturaleza del dato si es necesario)
    df_monthly = df.resample('MS').first()
    
    return df_monthly

# ==========================================
# 3. PROCESO PRINCIPAL
# ==========================================

def main():
    print("🚀 INICIANDO CONSTRUCCIÓN DE DATASET\n")
    
    # -----------------------------------
    # A. Datos de Estados Unidos (Fred/Yahoo)
    # -----------------------------------
    print("--- [A] Variables EE.UU. (Automatizado) ---")
    
    try:
        fred = Fred(api_key=FRED_API_KEY)
        us_data = pd.DataFrame()
        
        # A.1 FRED Series
        series_map = {
            'US_PCE': 'PCEPI',     # Gasto Consumo
            'US_IP': 'INDPRO',     # Producción Industrial
            'US_RER': 'RBUSBIS'    # Tipo de Cambio Real
        }
        
        for my_name, fred_code in series_map.items():
            print(f"   Fetching {my_name} ({fred_code})...")
            try:
                s = fred.get_series(fred_code)
                s.name = my_name
                us_data = us_data.join(s.to_frame(), how='outer')
            except Exception as e:
                print(f"      ❌ Error: {e}")

        # A.2 Yahoo Finance
        yf_tickers = {
            'US_VIX': '^VIX',
            'US_SP500': '^GSPC'
        }
        
        for my_name, ticker in yf_tickers.items():
            print(f"   Fetching {my_name} ({ticker})...")
            try:
                hist = yf.Ticker(ticker).history(period="max", interval="1mo")
                hist.index = hist.index.tz_localize(None) # Quitar timezone
                hist = hist[['Close']].rename(columns={'Close': my_name})
                us_data = us_data.join(hist, how='outer')
            except Exception as e:
                print(f"      ❌ Error: {e}")

        # Alinear fechas (Inicio de mes)
        us_data = us_data.resample('MS').first()
        print("   ✅ Variables US completas.")
        
    except Exception as e:
        print(f"❌ Error crítico en bloque US: {e}")
        return

    # -----------------------------------
    # B. Datos de Archivos (GitHub)
    # -----------------------------------
    print("\n--- [B] Variables EM y Manuales (Desde GitHub) ---")
    
    manual_data = pd.DataFrame()
    
    # B.1 Descargar y Procesar
    for key, filename in FILES.items():
        if download_github_file(filename):
            try:
                raw = pd.read_excel(filename)
                clean = clean_and_format(raw, name=key)
                
                # Seleccionar columna numérica (asumimos que es la relevante)
                num_cols = clean.select_dtypes(include=['number']).columns
                
                if len(num_cols) > 0:
                    # Renombrar columna principal
                    target_col = num_cols[0]
                    
                    if key == "FF4":
                        new_name = "US_FF4_SHOCK"
                        clean = clean[[target_col]].rename(columns={target_col: new_name})
                        
                    elif key == "EMBI":
                        # El archivo EMBI probablemente tenga multiples columnas (ARG, CHL, etc.)
                        # Guardamos todas con prefijo
                        clean = clean.add_prefix("EMBI_")
                        
                    elif "ACT" in key:
                        country = key.split("_")[1] # BRA, CHL, MEX
                        new_name = f"ACT_{country}"
                        clean = clean[[target_col]].rename(columns={target_col: new_name})
                    
                    manual_data = manual_data.join(clean, how='outer')
                    
                else:
                    print(f"      ⚠️ No se encontraron columnas numéricas en {filename}")
                    
            except Exception as e:
                print(f"      ❌ Error procesando {filename}: {e}")

    # B.2 Actividad Argentina (Proxy si falta)
    # Nota: No había archivo ACTIVIDAD-ARGENTINA.xlsx en la lista detectada.
    print("   Buscando Actividad Argentina (FRED Proxy)...")
    try:
        # Proxy: Industrial Production Argentina (Indec data via Fred)
        arg_act = fred.get_series('ARGPROINDMISMEI')
        arg_act.name = 'ACT_ARG'
        manual_data = manual_data.join(arg_act.to_frame(), how='outer')
        print("      ✅ Se usó 'ARGPROINDMISMEI' como proxy para ACT_ARG")
    except:
        print("      ⚠️ No se encontró datos para Argentina (Proxy falló)")

    # -----------------------------------
    # C. Fusión Final
    # -----------------------------------
    print("\n--- [C] Generando Master DataFrame ---")
    
    # Unir US y Manual
    # Asegurar índices compatibles
    if not us_data.empty:
        master = us_data.join(manual_data, how='outer')
    else:
        master = manual_data
    
    # Ordenar
    master = master.sort_index()
    
    # Filtrar desde 1990 en adelante (para evitar datos irrelevantes muy viejos)
    master = master['1990-01-01':]
    
    # Interpolar para rellenar huecos pequeños si es necesario, o dejar NaN
    # master = master.interpolate(method='linear', limit=1)

    print(f"   Dimensiones Finales: {master.shape}")
    print(f"   Rango Fechas: {master.index.min()} -> {master.index.max()}")
    print("\n   Variables:")
    for c in master.columns:
        print(f"   - {c}")
        
    # Exportar
    output_name = "Thesis_Master_Dataset.xlsx"
    master.to_excel(output_name)
    print(f"\n💾 EXPORTADO EXITOSAMENTE A: {output_name}")
    print("✅ Proceso Terminado.")

if __name__ == "__main__":
    main()

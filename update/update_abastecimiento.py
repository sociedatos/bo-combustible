import argparse
import json
from pathlib import Path
import time
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests


BASE_URL = 'https://abastecimiento.prod.anh.gob.bo/api'
STATIONS_URL = f'{BASE_URL}/estaciones'
DEPARTMENTS_URL = f'{BASE_URL}/departamentos'

PRODUCTS = {
    0: 'Gasolina',
    1: 'Diesel',
    2: 'Gasolina Premium',
    3: 'Diesel ULS',
}
DEPARTMENT_IDS = range(1, 10)
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0',
    'Accept': '*/*',
    'Connection': 'close',
}
TIMEOUT = 30
RETRY = 3

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / 'data_abastecimiento'
STATIONS_FILE = ROOT_DIR / 'stations.csv'

OUTPUT_COLUMNS = [
    'fecha_actualizacion',
    'id_eess',
    'fecha_actualizacion_sistema',
    'id_producto_abs',
    'saldo_litros',
    'saldo_estado',
    'con_venta',
    'fecha_ultima_venta',
    'despacho_en_curso',
    'fecha_hora_despacho',
    'seguimiento_id',
    'nombre',
    'direccion',
    'zona',
    'departamento_id',
    'lat',
    'lng',
]

DATE_COLUMNS = [
    'fecha_actualizacion',
    'fecha_actualizacion_sistema',
    'fecha_ultima_venta',
    'fecha_hora_despacho',
]


def fetch_stations(session, department_id, product_id):
    params = {
        'departamento': department_id,
        'producto': product_id,
    }

    for attempt in range(RETRY):
        try:
            response = session.get(STATIONS_URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            payload = response.json()

            if payload.get('strMensaje') != 'OK':
                return []

            rows = payload.get('oResultado')
            if rows is None:
                return []
            if not isinstance(rows, list):
                raise ValueError('oResultado is not a list')

            return rows
        except (requests.RequestException, ValueError, json.JSONDecodeError) as error:
            if attempt == RETRY - 1:
                raise RuntimeError(
                    'Could not download department {} product {}'.format(
                        department_id, product_id
                    )
                ) from error
            time.sleep(2 ** attempt)


def format_snapshot(rows, now=None):
    if not rows:
        raise RuntimeError('No data available to format snapshot')

    frame = pd.DataFrame(rows)

    rename_map = {
        'id': 'id_eess',
        'producto_id': 'id_producto_abs',
        'updated_at': 'fecha_actualizacion_sistema',
    }
    frame = frame.rename(columns={k: v for k, v in rename_map.items() if k in frame.columns})

    for col in OUTPUT_COLUMNS:
        if col not in frame.columns and col != 'fecha_actualizacion':
            frame[col] = None

    now = (
        pd.Timestamp.now(tz='America/La_Paz')
        if now is None
        else pd.Timestamp(now)
    )
    if now.tzinfo is not None:
        now = now.tz_convert('America/La_Paz').tz_localize(None)
    now = now.floor('s')

    frame['fecha_actualizacion'] = now
    snapshot = frame[OUTPUT_COLUMNS].copy()

    for column in DATE_COLUMNS:
        date_values = (
            snapshot[column]
            .astype('string')
            .str.split('.', regex=False)
            .str[0]
            .str.replace(' ', 'T', n=1)
        )
        snapshot[column] = pd.to_datetime(
            date_values,
            format='%Y-%m-%dT%H:%M:%S',
            errors='coerce',
        )

    # Deduplicate keeping last update per station and product
    snapshot = snapshot.drop_duplicates(
        subset=['id_eess', 'id_producto_abs'],
        keep='last'
    ).reset_index(drop=True)

    return snapshot


def download_snapshot(now=None):
    rows = []

    with requests.Session() as session:
        session.headers.update(HEADERS)

        for department_id in DEPARTMENT_IDS:
            for product_id in PRODUCTS:
                dept_rows = fetch_stations(session, department_id, product_id)
                if not dept_rows:
                    continue

                for row in dept_rows:
                    item = dict(row)
                    item['producto_id'] = product_id
                    rows.append(item)

    return format_snapshot(rows, now=now)


def parse_har(har_path, now=None):
    with open(har_path, 'r', encoding='utf-8') as f:
        har_data = json.load(f)

    entries = har_data.get('log', {}).get('entries', [])
    rows = []

    for entry in entries:
        req = entry.get('request', {})
        res = entry.get('response', {})
        url = req.get('url', '')
        parsed = urlparse(url)
        content = res.get('content', {})
        text = content.get('text', '')

        if not text:
            continue

        qs = parse_qs(parsed.query)

        # 1. /api/estaciones?departamento=X&producto=Y
        if parsed.path.rstrip('/') == '/api/estaciones':
            try:
                payload = json.loads(text)
                if payload.get('strMensaje') == 'OK' and isinstance(payload.get('oResultado'), list):
                    prod_id = int(qs.get('producto', [0])[0])
                    for item in payload['oResultado']:
                        row = dict(item)
                        row['producto_id'] = prod_id
                        rows.append(row)
            except Exception:
                pass

        # 2. /api/estaciones/<id>
        elif parsed.path.startswith('/api/estaciones/'):
            try:
                payload = json.loads(text)
                if payload.get('strMensaje') == 'OK' and isinstance(payload.get('oResultado'), dict):
                    station = payload['oResultado']
                    base_info = {k: v for k, v in station.items() if k != 'productos'}
                    for prod in station.get('productos', []):
                        row = dict(base_info)
                        row.update(prod)
                        rows.append(row)
            except Exception:
                pass

        # 3. /api/stream/estaciones?departamento=X&producto=Y
        elif parsed.path.rstrip('/') == '/api/stream/estaciones':
            prod_id = int(qs.get('producto', [0])[0])
            for line in text.splitlines():
                if line.startswith('data:'):
                    payload_str = line[5:].strip()
                    try:
                        events = json.loads(payload_str)
                        if isinstance(events, list):
                            for item in events:
                                row = dict(item)
                                row['producto_id'] = prod_id
                                rows.append(row)
                    except Exception:
                        pass

    if not rows:
        raise RuntimeError(f'No station records found in HAR file: {har_path}')

    return format_snapshot(rows, now=now)


def update_store(snapshot, now, data_dir=DATA_DIR):
    data_dir.mkdir(parents=True, exist_ok=True)
    filename = data_dir / '{}.csv'.format(now.strftime('%Y%W'))

    snapshot.to_csv(
        filename,
        mode='a',
        header=not filename.exists(),
        index=False,
    )
    return filename


def update_stations_store(snapshot, stations_file=STATIONS_FILE):
    station_cols = ['id_eess', 'nombre', 'direccion', 'departamento_id', 'lat', 'lng']
    if not all(col in snapshot.columns for col in station_cols):
        return

    new_stations = snapshot[station_cols].drop_duplicates(subset=['id_eess']).copy()
    new_stations = new_stations.rename(columns={
        'id_eess': 'id_eess_saldo',
        'lat': 'latitud',
        'lng': 'longitud',
        'nombre': 'nombreEstacion',
        'departamento_id': 'id_departamento',
    })
    new_stations['id_entidad'] = None

    if stations_file.exists():
        existing_df = pd.read_csv(stations_file)
        combined = pd.concat([existing_df, new_stations], ignore_index=True)
        combined = combined.drop_duplicates(subset=['id_eess_saldo'], keep='first')
        combined = combined.sort_values('id_eess_saldo').reset_index(drop=True)
    else:
        combined = new_stations.sort_values('id_eess_saldo').reset_index(drop=True)

    combined.to_csv(stations_file, index=False)
    return stations_file


def main():
    parser = argparse.ArgumentParser(
        description='Extract station and fuel balance data from ANH Abastecimiento portal or HAR archive.'
    )
    parser.add_argument(
        'har_file',
        nargs='?',
        default=None,
        help='Optional path to .har file. If omitted, downloads live data from API.'
    )
    parser.add_argument(
        '--data-dir',
        type=Path,
        default=DATA_DIR,
        help=f'Directory to append weekly CSV snapshots (default: {DATA_DIR})'
    )
    parser.add_argument(
        '--update-stations',
        action='store_true',
        help='Update stations.csv with station metadata'
    )
    parser.add_argument(
        '--output-csv',
        type=Path,
        default=None,
        help='Save output to a specific CSV file instead of weekly directory'
    )

    args = parser.parse_args()

    print('[!] start')

    if args.har_file:
        print(f'[*] parsing HAR archive: {args.har_file}')
        snapshot = parse_har(args.har_file)
    else:
        print(f'[*] downloading live snapshot from {BASE_URL}')
        snapshot = download_snapshot()

    now = snapshot['fecha_actualizacion'].iloc[0]

    if args.output_csv:
        args.output_csv.parent.mkdir(parents=True, exist_ok=True)
        snapshot.to_csv(args.output_csv, index=False)
        filename = args.output_csv
    else:
        filename = update_store(snapshot, now, data_dir=args.data_dir)

    if args.update_stations:
        update_stations_store(snapshot)
        print(f'[*] stations store updated: {STATIONS_FILE.relative_to(ROOT_DIR)}')

    print(
        '[*] extracted: {} rows ({} stations)'.format(
            len(snapshot), snapshot['id_eess'].nunique()
        )
    )
    print(f'[*] stored: {filename.relative_to(ROOT_DIR) if filename.is_relative_to(ROOT_DIR) else filename}')
    print('[!] finish')


if __name__ == '__main__':
    main()

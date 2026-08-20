import time
from pathlib import Path

import pandas as pd
import requests


BASE_URL = (
    'https://vsr11vpr08m22gb.anh.gob.bo:9443/WSMobile/v2/'
    'estaciones/9ADE86E5A083423EBE50C051F4DB9778'
)
PRODUCTS = {
    0: 'Gasolina',
    1: 'Diesel',
    2: 'Gasolina Premium',
    3: 'Diesel ULS',
}
DEPARTMENT_IDS = range(1, 10)
HEADERS = {
    'user-agent': 'Dart/3.4 (dart:io)',
    'Connection': 'close',
}
TIMEOUT = 30
RETRY = 3

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / 'data_discrete'

OUTPUT_COLUMNS = [
    'fecha_actualizacion',
    'id_eess',
    'fecha_actualizacion_sistema',
    'id_producto_abs',
    'fecha_ultima_venta',
    'despacho_en_curso',
    'fecha_hora_despacho',
    'saldo_estado',
    'con_venta',
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
            response = session.get(BASE_URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            payload = response.json()

            if payload.get('strMensaje') != 'OK':
                return []

            rows = payload.get('oResultado')
            if not isinstance(rows, list):
                raise ValueError('oResultado is not a list')

            return rows
        except (requests.RequestException, ValueError) as error:
            if attempt == RETRY - 1:
                raise RuntimeError(
                    'Could not download department {} product {}'.format(
                        department_id, product_id
                    )
                ) from error
            time.sleep(5 ** attempt)


def download_snapshot(now=None):
    frames = []

    with requests.Session() as session:
        session.headers.update(HEADERS)

        for department_id in DEPARTMENT_IDS:
            for product_id in PRODUCTS:
                rows = fetch_stations(session, department_id, product_id)
                if not rows:
                    continue

                frame = pd.DataFrame(rows)
                frame['producto_id'] = product_id
                frames.append(frame)

    if not frames:
        raise RuntimeError('The API returned no station data')

    snapshot = pd.concat(frames, ignore_index=True)
    snapshot = snapshot.rename(columns={
        'updated_at': 'fecha_actualizacion_sistema',
        'id': 'id_eess',
        'producto_id': 'id_producto_abs',
    })

    missing_columns = set(OUTPUT_COLUMNS[1:]) - set(snapshot.columns)
    if missing_columns:
        raise ValueError(
            'The API response is missing columns: {}'.format(
                ', '.join(sorted(missing_columns))
            )
        )

    now = (
        pd.Timestamp.now(tz='America/La_Paz')
        if now is None
        else pd.Timestamp(now)
    )
    if now.tzinfo is not None:
        now = now.tz_convert('America/La_Paz').tz_localize(None)
    now = now.floor('s')

    snapshot['fecha_actualizacion'] = now
    snapshot = snapshot[OUTPUT_COLUMNS].copy()

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

    return snapshot


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


def main():
    print('[!] start')

    snapshot = download_snapshot()
    now = snapshot['fecha_actualizacion'].iloc[0]
    filename = update_store(snapshot, now)

    print(
        '[*] downloaded: {} rows ({} stations)'.format(
            len(snapshot), snapshot['id_eess'].nunique()
        )
    )
    print('[*] stored: {}'.format(filename.relative_to(ROOT_DIR)))
    print('[!] finish')


if __name__ == '__main__':
    main()

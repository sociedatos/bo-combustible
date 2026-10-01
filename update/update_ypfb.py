import argparse
import json
from pathlib import Path
import re
import time

import pandas as pd
import requests


URL = 'https://consulta.ypfb.gob.bo/'
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:153.0) Gecko/20100101 Firefox/153.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Connection': 'close',
}
TIMEOUT = 30
RETRY = 3

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / 'data_ypfb'

DESPACHOS_COLUMNS = [
    'fecha_captura',
    'fecha_datos',
    'distrito_id',
    'distrito_codigo',
    'distrito_nombre',
    'producto_general',
    'producto_despachado',
    'familia',
    'familia_codigo',
    'cliente',
    'estacion',
    'placa_despacho',
    'cantidad_despachada',
    'unidad',
    'fecha_despacho_programado',
    'fecha_despacho_efectivo',
    'fecha_despacho_efectivo_hora',
    'despacho_confirmado',
    'fecha_despacho_difiere_de_programado',
    'ambito',
    'grupo',
    'gps',
]

PROGRAMACION_COLUMNS = [
    'fecha_captura',
    'fecha_datos',
    'distrito_id',
    'distrito_codigo',
    'distrito_nombre',
    'producto_general',
    'producto',
    'familia',
    'familia_codigo',
    'cliente',
    'volumen_programado',
    'canal',
    'grupo',
    'atendida',
]

RESUMEN_COLUMNS = [
    'fecha_captura',
    'fecha_datos',
    'distrito_id',
    'distrito_codigo',
    'distrito_nombre',
    'producto_general',
    'litros_programados',
    'litros_despachados',
    'litros_pendientes_confirmacion',
]


def extract_payload_from_html(html_text):
    match = re.search(
        r'<script type=["\']application/json["\'] id=["\']datos-publicos["\']>([\s\S]*?)</script>',
        html_text,
    )
    if not match:
        raise ValueError('No script tag with id="datos-publicos" found in HTML')
    return json.loads(match.group(1))


def fetch_live_payload():
    for attempt in range(RETRY):
        try:
            response = requests.get(URL, headers=HEADERS, timeout=TIMEOUT)
            response.raise_for_status()
            return extract_payload_from_html(response.text)
        except (requests.RequestException, ValueError, json.JSONDecodeError) as error:
            if attempt == RETRY - 1:
                raise RuntimeError('Could not fetch data from {}'.format(URL)) from error
            time.sleep(2 ** attempt)


def parse_har(har_path):
    with open(har_path, 'r', encoding='utf-8') as f:
        har_data = json.load(f)

    entries = har_data.get('log', {}).get('entries', [])
    for entry in entries:
        req_url = entry.get('request', {}).get('url', '')
        if 'consulta.ypfb.gob.bo' in req_url:
            text = entry.get('response', {}).get('content', {}).get('text', '')
            if text and 'datos-publicos' in text:
                return extract_payload_from_html(text)

    raise ValueError(f'No YPFB payload found in HAR archive: {har_path}')


def process_payload(payload, now=None):
    now = (
        pd.Timestamp.now(tz='America/La_Paz')
        if now is None
        else pd.Timestamp(now)
    )
    if now.tzinfo is not None:
        now = now.tz_convert('America/La_Paz').tz_localize(None)
    now = now.floor('s')

    fecha_datos = payload.get('fecha_datos')
    distritos_map = {str(d['id']): d for d in payload.get('distritos', [])}

    despachos = []
    programaciones = []
    resumenes = []

    for d_id, d_data in payload.get('datos', {}).items():
        dist_info = distritos_map.get(str(d_id), {})
        dist_cod = dist_info.get('codigo', '')
        dist_nom = dist_info.get('nombre', '')

        for prod_general, prod_data in d_data.items():
            # 1. Resumen
            res = prod_data.get('resumen')
            if res:
                item = dict(res)
                item['fecha_captura'] = now
                item['fecha_datos'] = fecha_datos
                item['distrito_id'] = int(d_id)
                item['distrito_codigo'] = dist_cod
                item['distrito_nombre'] = dist_nom
                item['producto_general'] = prod_general
                resumenes.append(item)

            # 2. Despachos
            for desp in prod_data.get('despachos', []):
                item = dict(desp)
                item['fecha_captura'] = now
                item['fecha_datos'] = fecha_datos
                item['distrito_id'] = int(d_id)
                item['distrito_codigo'] = dist_cod
                item['distrito_nombre'] = dist_nom
                item['producto_general'] = prod_general
                despachos.append(item)

            # 3. Programación
            for prog in prod_data.get('programacion', []):
                item = dict(prog)
                item['fecha_captura'] = now
                item['fecha_datos'] = fecha_datos
                item['distrito_id'] = int(d_id)
                item['distrito_codigo'] = dist_cod
                item['distrito_nombre'] = dist_nom
                item['producto_general'] = prod_general
                programaciones.append(item)

    df_desp = pd.DataFrame(despachos)
    df_prog = pd.DataFrame(programaciones)
    df_res = pd.DataFrame(resumenes)

    for col in DESPACHOS_COLUMNS:
        if col not in df_desp.columns:
            df_desp[col] = None
    for col in PROGRAMACION_COLUMNS:
        if col not in df_prog.columns:
            df_prog[col] = None
    for col in RESUMEN_COLUMNS:
        if col not in df_res.columns:
            df_res[col] = None

    df_desp = df_desp[DESPACHOS_COLUMNS].copy()
    df_prog = df_prog[PROGRAMACION_COLUMNS].copy()
    df_res = df_res[RESUMEN_COLUMNS].copy()

    return df_desp, df_prog, df_res, now


def append_store_section(df, now, section_name, dedupe_keys, data_dir=DATA_DIR):
    target_dir = data_dir / section_name
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = target_dir / '{}.csv'.format(now.strftime('%Y%W'))

    if filename.exists():
        existing = pd.read_csv(filename)
        combined = pd.concat([existing, df], ignore_index=True)
        if dedupe_keys:
            valid_keys = [k for k in dedupe_keys if k in combined.columns]
            combined = combined.drop_duplicates(subset=valid_keys, keep='last')
        combined.to_csv(filename, index=False)
    else:
        df.to_csv(filename, index=False)

    return filename


def update_ypfb_store(df_desp, df_prog, df_res, now, data_dir=DATA_DIR):
    fn_desp = append_store_section(
        df_desp,
        now,
        'despachos',
        dedupe_keys=[
            'fecha_datos',
            'placa_despacho',
            'cliente',
            'producto_despachado',
            'fecha_despacho_efectivo_hora',
        ],
        data_dir=data_dir,
    )
    fn_prog = append_store_section(
        df_prog,
        now,
        'programacion',
        dedupe_keys=[
            'fecha_datos',
            'fecha_programacion',
            'distrito_id',
            'cliente',
            'producto',
        ],
        data_dir=data_dir,
    )
    fn_res = append_store_section(
        df_res,
        now,
        'resumen',
        dedupe_keys=[
            'fecha_datos',
            'distrito_id',
            'producto_general',
        ],
        data_dir=data_dir,
    )
    return fn_desp, fn_prog, fn_res


def main():
    parser = argparse.ArgumentParser(
        description='Extract fuel dispatch and nomination data from YPFB monitoring portal or HAR archive.'
    )
    parser.add_argument(
        'har_file',
        nargs='?',
        default=None,
        help='Optional path to .har file. If omitted, downloads live data from https://consulta.ypfb.gob.bo/'
    )
    parser.add_argument(
        '--data-dir',
        type=Path,
        default=DATA_DIR,
        help=f'Base directory to store YPFB datasets (default: {DATA_DIR})'
    )

    args = parser.parse_args()

    print('[!] start YPFB update')

    if args.har_file:
        print(f'[*] parsing HAR archive: {args.har_file}')
        payload = parse_har(args.har_file)
    else:
        print(f'[*] downloading live data from {URL}')
        payload = fetch_live_payload()

    df_desp, df_prog, df_res, now = process_payload(payload)

    fn_desp, fn_prog, fn_res = update_ypfb_store(df_desp, df_prog, df_res, now, data_dir=args.data_dir)

    print(f'[*] despachos: {len(df_desp)} rows -> {fn_desp.relative_to(ROOT_DIR) if fn_desp.is_relative_to(ROOT_DIR) else fn_desp}')
    print(f'[*] programacion: {len(df_prog)} rows -> {fn_prog.relative_to(ROOT_DIR) if fn_prog.is_relative_to(ROOT_DIR) else fn_prog}')
    print(f'[*] resumen: {len(df_res)} rows -> {fn_res.relative_to(ROOT_DIR) if fn_res.is_relative_to(ROOT_DIR) else fn_res}')
    print('[!] finish YPFB update')


if __name__ == '__main__':
    main()

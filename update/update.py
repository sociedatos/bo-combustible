import os

import numpy as np
import pandas as pd

import requests

import asyncio
import async_timeout

import aiohttp


BASE_URL = 'https://vsr11vpr08m22gb.anh.gob.bo:9443/WSMobile/v1'
STATION_LIST_URL = BASE_URL + '/EstacionesXprod/F761D63AC28406573E20A24CB1DB2EC6/{}/{}'
STATION_URL = BASE_URL + '/EstacionesSaldo/F761D63AC28406573E20A24CB1DB2EC6/{}/{}'

HEADERS = {
    'user-agent': 'Dart/3.4 (dart:io)',
    'Connection': 'close',
}
TIMEOUT = 5


###############################################################################
# update
###############################################################################

def update_station_list():
    stations = []
    for cod_prod in range(3 + 1):
        for cod_dept in range(1, 9 + 1):
            base_url = STATION_LIST_URL.format(cod_dept, cod_prod)

            req = requests.get(base_url, headers=HEADERS, timeout=30)
            req_stationss = req.json()['oResultado']

            req_stationss = pd.DataFrame(req_stationss)
            req_stationss['product_code'] = cod_prod

            stations.append(req_stationss)

    stations_df = pd.concat(stations).reset_index(drop=True)
    stations_df = stations_df[
        ~stations_df[['id_eess_saldo', 'product_code']].duplicated(keep='first')
    ]

    stations_df[[
        'id_eess_saldo',
        'id_entidad',
        'id_departamento',
        'product_code'
    ]] = stations_df[[
        'id_eess_saldo',
        'id_entidad',
        'id_departamento',
        'product_code'
    ]].astype(int)

    return stations_df


async def fetch_station(semaphore, session, cod_prod, station_id):
    base_url = STATION_URL.format(station_id, cod_prod)

    async with semaphore:
        try:
            async with async_timeout.timeout(TIMEOUT):
                async with session.get(base_url, headers=HEADERS) as req:
                    req.raise_for_status()
                    req_data = await req.json()

                    if req_data['strMensaje'] == 'OK':
                        return req_data['oResultado']

        except:
            await asyncio.sleep(.05)

    return


async def update_stations(cod_prod, station_ids, max_concurrency=1):
    semaphore = asyncio.Semaphore(max_concurrency)

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_station(semaphore, session, cod_prod, _) for _ in station_ids
        ]
        res = []

        for task in asyncio.as_completed(tasks):
            station_res = await task
            res.append(station_res)

        return res


###############################################################################
# store
###############################################################################

def update_stations_store(stations_df):
    stations_df = stations_df[[
        'id_eess_saldo',
        'id_entidad',
        'latitud',
        'longitud',
        'nombreEstacion',
        'direccion',
        'id_departamento'
    ]]

    stored_df = pd.read_csv('./stations.csv')
    stored_df = pd.concat([stored_df, stations_df])

    stored_df = stored_df[
        ~stored_df['id_eess_saldo'].duplicated(keep='last')
    ]
    stored_df = stored_df.sort_values('id_eess_saldo')
    stored_df.to_csv('./stations.csv', index=False)


def update_store(sal2_df, now):
    fn = './data/{}.csv'.format(now.strftime('%Y%W'))

    if not os.path.isfile(fn):
        sal2_df.to_csv(fn)
        return

    stored_df = pd.read_csv(fn)
    stored_df = pd.concat([
        stored_df, sal2_df.reset_index()
    ])

    stored_df.to_csv(fn, index=False)


###############################################################################
# run
###############################################################################

if __name__ == '__main__':
    stations_df = update_station_list()
    saldos = []

    for cod_prod, stations_prod in stations_df.groupby('product_code'):
        results = asyncio.run(
            update_stations(cod_prod, stations_prod['id_eess_saldo'].values, max_concurrency=20)
        )
        saldos.extend(results)

    sal2_df = pd.DataFrame([__ for _ in saldos if _ for __ in _])

    now = pd.to_datetime('now', utc=True)
    now = now.tz_convert("Etc/GMT+4").tz_localize(None).floor('s')

    sal2_df['fecha_actualizacion'] = now

    sal2_df = sal2_df.set_index(['fecha_actualizacion', 'id_eess', 'id_producto_bsa'])[[
        'saldo_octano', 'saldo_bsa', 'saldo_planta'
    ]].sort_index()

    update_store(sal2_df, now)
    update_stations_store(stations_df)

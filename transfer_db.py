from psycopg2 import connect
from aiochclient import ChClient
from aiohttp import ClientSession
from app import segments
from math import ceil
import asyncio


async def put(table, lst):
    async with ClientSession() as s:
        client = ChClient(s, database='offers_dims',
                          url='http://10.199.13.111:8123')
        try:
            await client.execute(f'INSERT INTO {table} \
                (source, link, price, area, hash, date, diap, storage, operation) \
                VALUES', *lst)
        except Exception as e:
            print(f'{e.__class__.__name__}: {e}')
            print('!!!ERROR!!!')


def get(table, offset):
    with connect(**{'user': 'rway', 'password': 'rway', 'dbname': 'offers_dims', 'host': '10.199.13.111'}) as conn:
        with conn.cursor() as cur:
            q = f'SELECT source, link, price, area, hash, date, diap, storage, operation FROM {table} LIMIT 1000 OFFSET {offset}'
            cur.execute(q)
            return cur.fetchall()


def get_length(table):
    with connect(**{'user': 'rway', 'password': 'rway', 'dbname': 'offers_dims', 'host': '10.199.13.111'}) as conn:
        with conn.cursor() as cur:
            q = f'SELECT COUNT(*) FROM {table}'
            cur.execute(q)
            return cur.fetchone()[0]


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    for table in segments.values():
        length = get_length(table)
        for offset in range(ceil(length / 1000)):
            print(table, offset * 1000, length)
            data = get(table, offset * 1000)
            data = list(map(lambda x: list(
                map(lambda y: int(y) if isinstance(y, bool) else y, x)), data))
            loop.run_until_complete(put(table, data))

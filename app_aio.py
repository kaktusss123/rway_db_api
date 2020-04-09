import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession
from flask import Flask, request
from hashlib import sha256
from json import dumps
from datetime import date

app = Flask(__name__)
segments = {
    'Коммерческая Недвижимость': 'commerce_offers',
    'Земельные участки': 'land_offers',
    'Готовый бизнес': 'business_offers',
    'Жилая недвижимость': 'flat_offers',
    'Загородная недвижимость': 'house_offers',
    'Гаражи и машиноместа': 'garage_offers'
}


async def get(lst: list) -> str:
    wrapper = "'"  # обертка для f-строки, а то питон ругается
    seg = segments[lst[0]['segment']]
    keys = ('source', 'link', 'price', 'area')
    hash_to_id = {sha256("__".join((str(el[key]) for key in keys)).encode(
        'utf-8')).hexdigest(): el['id'] for el in lst}
    async with ClientSession() as s:
        client = ChClient(s, url='http://10.199.13.111:8123',
                          database='offers_dims')
        if not await client.is_alive():
            return 'DB connection error'
        q = f'SELECT hash, diap, storage, operation FROM {seg} WHERE hash IN ({", ".join(map(lambda x: f"{wrapper}{x}{wrapper}", hash_to_id.keys()))})'
        data = await client.fetch(q)
    selected = list(map(lambda x: x[0], data))
    res = [{'id': id, 'exists': False, 'diap': None, 'storage': None, 'operation': None}
           for h, id in hash_to_id.items() if h not in selected]
    res += [{'id': hash_to_id[h], 'exists': True, 'diap': d,
             'storage': s, 'operation': o} for h, d, s, o in map(lambda x: x.values(), data)]
    return dumps(res)


async def put(lst: list) -> str:
    keys = ('source', 'link', 'price', 'area')
    async with ClientSession() as s:
        client = ChClient(s, database='offers_dims',
                          url='http://10.199.13.111:8123')
        if not await client.is_alive():
            return {'status': 'DB connection error'}
        for el in lst:
            el['operation'] = int(el.get('operation')) if el.get(
                'operation') is not None else None
            el['storage'] = int(el.get('storage')) if el.get(
                'storage') is not None else None
            el['diap'] = int(el.get('diap')) if el.get(
                'diap') is not None else None
            try:
                await client.execute(f'INSERT INTO {segments[el["segment"]]} \
                (source, link, price, area, hash, date, diap, storage, operation) \
                VALUES ', (el["source"], el["link"], el["price"], el["area"],
                           sha256("__".join((str(el[key]) for key in keys)).encode("utf-8")).hexdigest(), date.today(), el.get("diap"), el.get("storage"), el.get("operation")))
            except Exception as e:
                print(f'{e.__class__.__name__}: {e}')
        await client.execute(f'OPTIMIZE TABLE {segments[el["segment"]]}')
    return {'status': 'Success'}


@app.route('/put', methods=['POST'])
def put_api():
    loop = asyncio.new_event_loop()
    res = loop.run_until_complete(put(request.json))
    loop.close()
    return res


@app.route('/get', methods=['POST'])
def get_api():
    loop = asyncio.new_event_loop()
    res = loop.run_until_complete(get(request.json))
    loop.close()
    return res


if __name__ == "__main__":
    # app.run('10.199.13.111', 9515)
    app.run()

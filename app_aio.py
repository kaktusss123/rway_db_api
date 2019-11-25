import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession
from flask import Flask, request
from hashlib import sha256
from json import dumps

app = Flask(__name__)
segments = {
    'Коммерческая Недвижимость': 'commerce_offers'
}


async def get(lst: list) -> str:
    wrapper = "'"  # обертка для f-строки, а то питон ругается
    seg = segments[lst[0]['segment']]
    keys = ('source', 'link', 'price', 'area')
    hash_to_id = {sha256("__".join((str(el[key]) for key in keys)).encode('utf-8')).hexdigest(): el['id'] for el in lst}
    async with ClientSession() as s:
        client = ChClient(s, url='http://10.199.13.111:8123', database='offers_dims')
        if not await client.is_alive():
            return 'DB connection error'
        q = f'SELECT hash FROM {seg} WHERE hash IN ({", ".join(map(lambda x: f"{wrapper}{x}{wrapper}", hash_to_id.keys()))})'
        fetch = await client.fetch(q)
    selected = list(map(lambda x: x[0], fetch))
    res = [{'id': id, 'exists': h in selected} for h, id in hash_to_id.items()]
    return dumps(res)


async def put(lst: list) -> str:
    keys = ('source', 'link', 'price', 'area')
    async with ClientSession() as s:
        client = ChClient(s, database='offers_dims')
        if not await client.is_alive():
            return 'DB connection error'
        for el in lst:
            try:
                await client.execute(f'INSERT INTO {segments[el["segment"]]} \
                (source, link, price, area, hash) \
                VALUES (\'{el["source"]}\', \'{el["link"]}\', {el["price"]}, {el["area"]}, \
                \'{sha256("__".join((str(el[key]) for key in keys)).encode("utf-8")).hexdigest()}\')')
            except:
                pass
    return 'Success'
        
        
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
    app.run('10.199.13.111', 9515)
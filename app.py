import psycopg2
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


def put(lst: list) -> str:
    keys = ('source', 'link', 'price', 'area')
    with psycopg2.connect(**{'user': 'rway', 'password': 'rway', 'dbname': 'offers_dims', 'host': '10.199.13.111'}) as conn:
        with conn.cursor() as cur:
            for el in lst:
                cur.execute(f'INSERT INTO {segments[el["segment"]]} \
                (source, link, price, area, hash, date, diap, storage, operation) \
                VALUES (\'{el["source"]}\', \'{el["link"]}\', {el["price"]}, {el["area"]}, \
                \'{sha256("__".join((str(el[key]) for key in keys)).encode("utf-8")).hexdigest()}\', \'{date.today()}\', \'{el.get("diap")}\', \'{el.get("storage")}\', \'{el.get("operation")}\') \
                ON CONFLICT DO NOTHING')
        conn.commit()
    return {'status': 'Success'}


def get(lst: list) -> str:
    wrapper = "'"  # обертка для f-строки, а то питон ругается
    seg = segments[lst[0]['segment']]
    keys = ('source', 'link', 'price', 'area')
    hash_to_id = {sha256("__".join((str(el[key]) for key in keys)).encode(
        'utf-8')).hexdigest(): el['id'] for el in lst}
    with psycopg2.connect(**{'user': 'rway', 'password': 'rway', 'dbname': 'offers_dims', 'host': '10.199.13.111'}) as conn:
        with conn.cursor() as cur:
            q = f'SELECT hash, diap, storage, operation FROM {seg} WHERE hash IN ({", ".join(map(lambda x: f"{wrapper}{x}{wrapper}", hash_to_id.keys()))})'
            cur.execute(q)
            data = cur.fetchall()
            selected = list(map(lambda x: x[0], data))
            res = [{'id': id, 'exists': False, 'diap': None, 'storage': None, 'operation': None}
                   for h, id in hash_to_id.items() if h not in selected]
            res += [{'id': hash_to_id[h], 'exists': True, 'diap': d,
                     'storage': s, 'operation': o} for h, d, s, o in data]
    return dumps(res)


@app.route('/put', methods=['POST'])
def put_api():
    return put(request.json)


@app.route('/get', methods=['POST'])
def get_api():
    return get(request.json)


if __name__ == "__main__":
    app.run('10.199.13.111', 9514)


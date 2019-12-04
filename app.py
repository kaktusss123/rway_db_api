import psycopg2
from flask import Flask, request
from hashlib import sha256
from json import dumps
from datetime import date

app = Flask(__name__)
segments = {
    'Коммерческая Недвижимость': 'commerce_offers'
}

def put(lst: list) -> str:
    keys = ('source', 'link', 'price', 'area')
    with psycopg2.connect(**{'user': 'rway', 'password': 'rway', 'dbname': 'offers_dims', 'host': '10.199.13.111'}) as conn:
        with conn.cursor() as cur:
            for el in lst:
                cur.execute(f'INSERT INTO {segments[el["segment"]]} \
                (source, link, price, area, hash, date) \
                VALUES (\'{el["source"]}\', \'{el["link"]}\', {el["price"]}, {el["area"]}, \
                \'{sha256("__".join((str(el[key]) for key in keys)).encode("utf-8")).hexdigest()}\', \'{date.today()}\') \
                ON CONFLICT DO NOTHING')
        conn.commit()
    return 'Success'


def get(lst: list) -> str:
    wrapper = "'"  # обертка для f-строки, а то питон ругается
    seg = segments[lst[0]['segment']]
    keys = ('source', 'link', 'price', 'area')
    hash_to_id = {sha256("__".join((str(el[key]) for key in keys)).encode('utf-8')).hexdigest(): el['id'] for el in lst}
    with psycopg2.connect(**{'user': 'rway', 'password': 'rway', 'dbname': 'offers_dims', 'host': '10.199.13.111'}) as conn:
        with conn.cursor() as cur:
            q = f'SELECT hash FROM {seg} WHERE hash IN ({", ".join(map(lambda x: f"{wrapper}{x}{wrapper}", hash_to_id.keys()))})'
            cur.execute(q)
            selected = list(map(lambda x: x[0], cur.fetchall()))
            res = [{'id': id, 'exists': h in selected} for h, id in hash_to_id.items()]
    return dumps(res)
        

@app.route('/put', methods=['POST'])
def put_api():
    return put(request.json)

@app.route('/get', methods=['POST'])
def get_api():
    return get(request.json)

if __name__ == "__main__":
    app.run()
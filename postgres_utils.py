import psycopg2
import logging
import os
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from datetime import datetime
import shelve


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

shelve_file = os.getenv('SHELVE_FILE')

with shelve.open(shelve_file) as db:
    if len(db.keys()) > 0:
        preview_data = db
        logging.info(f"preview_data last value: {preview_data}")
    else:
        preview_data = {}
        logging.info(f"preview_data last value: {preview_data}")

DDBB_INFO = {
    "user": os.getenv('POSTGRES_USER', 'postgres'),
    "password": os.getenv('POSTGRES_PASSWORD', ''),
    "host": os.getenv('POSTGRES_HOST', 'localhost'),
    "port": os.getenv('POSTGRES_PORT', 5432),
    "database": os.getenv('POSTGRES_DB', 'postgres'),
}

def get_connection_info(asset_id):
    try:
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor() as cur:
                cur.execute(f""" select ip, port, a.manufacturer 
                from elliot.asset_plc ap join elliot.asset a on ap.asset_id = a.id 
                where id = {asset_id}""")
                connection_data = cur.fetchall()
        return {'ip': connection_data[0][0], 'port': connection_data[0][1], 'manufacturer': connection_data[0][2]}
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")


def get_tags_info(asset_id, connection_data):
    try:
        if connection_data['manufacturer'] == "siemens":
            columns = "m.id, m.type_id, dpi.db_num, dpi.offset, dpi.type"
        else:
            columns = " m.id, m.type_id, dpi.address, dpi.type"
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor(cursor_factory = RealDictCursor) as cur:
                cur.execute(f"""select {columns} 
                from elliot.asset a join elliot.metric m on a.id = m.asset_id 
                join elliot.dms_plcs_info dpi on m.id = dpi.metric_id 
                where a.id = {asset_id}
                order by  SUBSTRING ( dpi.address ,1 , 1 ), SUBSTRING ( SPLIT_PART(dpi.address, '.', 1) ,2)::numeric """)
                tags_info = cur.fetchall()
        types = get_tags_types(asset_id)
        for tag in tags_info:
            types[tag['type']].append(tag)
        return types
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")


def get_tags_types(asset_id):
    types = {}
    try:
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor() as cur:
                cur.execute(f'''select dpi."type"
                                from elliot.asset a join elliot.metric m on a.id = m.asset_id 
                                join elliot.dms_plcs_info dpi on m.id = dpi.metric_id 
                                where a.id = {asset_id}
                                group by dpi."type"''')
                types_resp = cur.fetchall()
        for type in types_resp:
            types[f'{type[0]}'] = []
        return types
    except Exception as e:
        logging.exception(e)


def insert_data(tags_values):
    data_to_insert_numeric = []
    data_to_insert_text = []
    try:
        ts = datetime.now()
        for tag_type in tags_values:
            if tag_type != 'Bit_word':
                for tag in tags_values[tag_type]:
                    try:
                        with shelve.open(shelve_file) as preview_data:
                            if tags_values[tag_type][tag]['metric_type'] is None or tag not in preview_data or (tags_values[tag_type][tag]['metric_type'] in [2, 3] and tags_values[tag_type][tag]['value'] != preview_data[tag]):
                                if 'String' in tags_values[tag_type][tag]['type']:
                                    data = [tags_values[tag_type][tag]['id'], ts, tags_values[tag_type][tag]['value']]
                                    data_to_insert_text.append(tuple(data))
                                else:
                                    data = [tags_values[tag_type][tag]['id'], ts, tags_values[tag_type][tag]['value']]
                                    data_to_insert_numeric.append(tuple(data))
                            
                            preview_data[tag] = tags_values[tag_type][tag]['value']

                    except Exception as e:
                        logging.exception(e)
                        continue
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor() as cur:
                if len(data_to_insert_numeric) > 0:
                    query = '''insert into elliot.metric_numeric_data (metric_id, ts, value) values %s
                    on conflict do nothing'''
                    psycopg2.extras.execute_values(cur, query, data_to_insert_numeric)
                    con.commit()
                    logging.info('Numeric measurements inserted')
                if len(data_to_insert_text) > 0:
                    query = '''insert into elliot.metric_text_data (metric_id, ts, value) values %s
                                    on conflict do nothing'''
                    psycopg2.extras.execute_values(cur, query, data_to_insert_text)
                    con.commit()
                    logging.info('Text measurements inserted')
    except Exception as e:
        logging.exception(e)

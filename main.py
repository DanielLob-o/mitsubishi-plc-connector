import time
from postgres_utils import get_connection_info, get_tags_info, insert_data
from plc_connector import connect, read_tags_value
import logging
import os

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
plc_id =  os.getenv('PLC_ID', 3)
sleep_time = os.getenv('SLEEP_TIME', 10)

def main():
    try:
        connection_data = get_connection_info(int(plc_id))
        tags_data = get_tags_info(int(plc_id), connection_data)
        pymc3e = connect(connection_data)
        while True:
            try:
                tags_values = read_tags_value(tags_data, pymc3e)
                insert_data(tags_values)
            except Exception as e:
                logging.exception(e)
                try:
                    logging.error("Closing connection")
                    pymc3e.close()
                except:
                    logging.error("No se puede cerrar la conexión")
                time.sleep(60)
                logging.info("Trying to connect")
                pymc3e = connect(connection_data)
                if pymc3e is not None:
                    logging.info("Connected")
                else:
                    logging.info("Cant Connect")
            finally:
                time.sleep(int(sleep_time))
    except Exception as e:
        logging.exception(e)




if __name__ == "__main__":
    main()

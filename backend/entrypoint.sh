#!/bin/sh
python -c "
import os, time, psycopg2
while True:
    try:
        psycopg2.connect(dbname=os.environ.get('DB_NAME'), user=os.environ.get('DB_USER'), password=os.environ.get('DB_PASSWORD'), host=os.environ.get('DB_HOST'), port=os.environ.get('DB_PORT'))
        break
    except psycopg2.OperationalError:
        time.sleep(2)
"
python manage.py makemigrations api
python manage.py migrate
python create_superuser.py
exec python manage.py runserver 0.0.0.0:8001
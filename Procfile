web: gunicorn -w 4 src.server:app
worker: celery -A src.app.celery worker --loglevel=info
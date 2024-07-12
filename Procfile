web: gunicorn -w 4 src.app:app
worker: celery -A src.app.celery worker --loglevel=info --concurrency=2
FROM python:3-alpine

WORKDIR /usr/src/data_grap

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY app/   ./app/

ENV PYTHONPATH "${PYTHONPATH}:/usr/src/data_grap"

CMD [ "python", "./app/run.py" ]

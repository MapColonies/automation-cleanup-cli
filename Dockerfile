FROM python:3.8

WORKDIR /app/cleanup

COPY . /app/cleanup/

RUN pip3 install --no-cache-dir -r requirements.txt

ENV CONF_FILE =$CONF_FILE

ENV LOGS_FILE_PATH =$LOGS_FILE_PATH

CMD ["python","cleanup_script.py"]
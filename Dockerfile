FROM python:3.6

RUN cp /usr/share/zoneinfo/Europe/Berlin /etc/localtime

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY link_mysql_database.py /app/link_mysql_database.py
RUN chmod a+x /app/link_mysql_database.py \
 && touch /app/config.ini

ENTRYPOINT ["/usr/local/bin/python3", "/app/link_mysql_database.py", "-c", "/app/config.ini"]

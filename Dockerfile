FROM phusion/baseimage:0.11

# Deactivate interactive configuration steps during package installation
ENV DEBIAN_FRONTEND noninteractive

# Upgrade Ubuntu for security patches
RUN apt-get update \
 && apt-get upgrade -y -o Dpkg::Options::="--force-confold" \
 # Install timezone info
 && apt-get install -y tzdata \
 # Setup timezone
 && cp /usr/share/zoneinfo/Europe/Berlin /etc/localtime \
 # Remove timezone info
 && apt-get remove -y tzdata \
 # Cleanup after APT
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Setup PIP
# RUN install_clean python3-pip python3-setuptools python3-wheel \
#  && python3 -m pip install --upgrade pip

# Install additional OS packages
RUN install_clean \
        python3-pymysql

# Install application requirements
# COPY requirements.txt /app/requirements.txt
# RUN pip install --user -r /app/requirements.txt

COPY *.py /app/
RUN touch /app/config.ini

# Set working directory
WORKDIR /app

# Setup image start
# ENTRYPOINT ["/sbin/my_init"]
ENTRYPOINT ["/usr/bin/python3", "/app/link_mysql_database.py", "-c", "/app/config.ini"]

# Add labels to the image
LABEL org.opencontainers.image.title="Mastersign MySQL Federated"
LABEL org.opencontainers.image.description="Link tables from one MySQL server as federated tables into another MYSQL server."
LABEL org.opencontainers.image.source="https://github.com/mastersign/mysql-federated"
LABEL org.opencontainers.image.documentation="https://github.com/mastersign/mysql-federated/blob/master/README.md"

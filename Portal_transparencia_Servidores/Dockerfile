FROM python:3.8.10
ENV DEBIAN_FRONTEND noninteractive
ENV GECKODRIVER_VER v0.29.0
ENV FIREFOX_VER 87.0
RUN apt update && apt upgrade -y && apt install -y firefox-esr

# Add latest FireFox
RUN apt install -y libx11-xcb1 libdbus-glib-1-2 && curl -sSLO https://download-installer.cdn.mozilla.net/pub/firefox/releases/${FIREFOX_VER}/linux-x86_64/en-US/firefox-${FIREFOX_VER}.tar.bz2 && tar -jxf firefox-* && mv firefox /opt/ && chmod 755 /opt/firefox && chmod 755 /opt/firefox/firefox

# Add geckodriver
RUN curl -sSLO https://github.com/mozilla/geckodriver/releases/download/${GECKODRIVER_VER}/geckodriver-${GECKODRIVER_VER}-linux64.tar.gz && tar zxf geckodriver-*.tar.gz && mv geckodriver /usr/bin/ && ln -s /usr/bin/geckodriver /usr/local/bin/geckodriver
RUN mkdir /datasets && mkdir /apps && mkdir /apps/utils
COPY requirements.txt /apps
RUN apt-get install libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit
RUN pip install --no-cache-dir --upgrade -r /apps/requirements.txt
RUN apt install unixodbc-dev --yes
RUN pip install pyodbc

COPY main.py /apps
COPY servidores.sh /apps
COPY conector.py /apps
COPY db.ini /apps
COPY utils-crawler/* /apps/utils/

ENV FONTE "PORTAL_TRANSPARENCIA"
ENV BASE "servidores" 
ENV CATEGORIA "coletor"
ENV NOME_COLETOR "portal-transparencia-br-servidores-executivo-crawler"

WORKDIR /apps
VOLUME ${DATALAKE_PATH}:/datalake 

CMD python main.py

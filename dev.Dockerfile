FROM tiangolo/uwsgi-nginx-flask:python3.7

RUN  git config --global http.sslVerify false && \
     mkdir -p /home/nginx/.cloudvolume/secrets && chown -R nginx /home/nginx && usermod -d /home/nginx -s /bin/bash nginx

RUN python -m pip install --upgrade pip
COPY dev_requirements.txt /app/.
RUN  pip install -r dev_requirements.txt
COPY timeout.conf /etc/nginx/conf.d/
COPY . /app
 
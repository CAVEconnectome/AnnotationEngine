FROM tiangolo/uwsgi-nginx-flask:python3.9

RUN apt-get update && apt-get install -y gcc curl ca-certificates \
     && rm -rf /var/lib/apt/lists/*

RUN pip install uv

# Enable bytecode compilation and avoid hardlinks in mounted cache volumes.
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=0

RUN git config --global http.sslVerify false && \
          mkdir -p /home/nginx/.cloudvolume/secrets && \
          chown -R nginx /home/nginx && \
          usermod -d /home/nginx -s /bin/bash nginx

WORKDIR /app

# Install runtime dependencies from the lockfile before copying source.
COPY uv.lock pyproject.toml ./
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN --mount=type=cache,target=/root/.cache/uv \
     UV_VENV_ARGS="--system-site-packages" uv sync --frozen --no-install-project --no-default-groups

ENV UWSGI_INI /app/uwsgi.ini
ENV PATH /app/.venv/bin:/home/nginx/google-cloud-sdk/bin:/root/google-cloud-sdk/bin:$PATH
ENV PYTHONNOUSERSITE=1

COPY timeout.conf /etc/nginx/conf.d/
COPY . /app
 
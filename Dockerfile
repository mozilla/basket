FROM debian:sid

RUN adduser --uid 1000 --disabled-password --gecos '' --no-create-home webdev
WORKDIR /app

EXPOSE 8000
CMD ["bin/run-prod.sh"]

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential python2.7 libpython2.7 python-dev python-setuptools \
        python-pip gettext python-mysqldb xmlsec1 libffi-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

# Install app
COPY requirements /app/requirements
RUN pip install --require-hashes --no-cache-dir -r requirements/prod.txt

COPY . /app
RUN DEBUG=False SECRET_KEY=foo ALLOWED_HOSTS=localhost, DATABASE_URL=sqlite:// SUPERTOKEN=bar \
    ./manage.py collectstatic --noinput

# Change User
RUN chown webdev.webdev -R .
USER webdev

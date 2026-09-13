FROM apache/airflow:2.10.5-python3.11

USER root
COPY docker/airflow/requirements.txt /tmp/requirements.txt
USER airflow

# Constrain against Airflow 2.10.5's own pins so an unconstrained GE/psycopg2
# install can't silently downgrade SQLAlchemy/marshmallow/etc. under Airflow's
# feet (ADR-4's whole point was avoiding exactly this class of breakage).
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    -r /tmp/requirements.txt

# code/ is also bind-mounted at runtime (see docker-compose.yml) so config/DAG
# edits take effect without a rebuild; this COPY+install just bakes the package
# metadata so `deriv_pipeline` is importable even before the mount is live.
COPY --chown=airflow:root . /opt/deriv/code
RUN pip install --no-cache-dir -e /opt/deriv/code

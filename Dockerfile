FROM python:3.12-slim
LABEL authors="Mathis Ersted Rasmussen"
ARG SERVICE_NAME

# Setup env paths
ENV APP_BASE_DIR="/opt/RadDeploy"
ENV APP_DIR=/opt/RadDeploy/${SERVICE_NAME}
ENV DEPENDENCIES_DIR=$APP_BASE_DIR/dependencies

# Setup config paths
ENV CONF_DIR=$APP_DIR/conf.d
ENV CURRENT_CONF=$APP_DIR/current_config.yaml

# Make directories
RUN mkdir -p $APP_BASE_DIR $APP_DIR $DEPENDENCIES_DIR
RUN chmod 753 -R $APP_BASE_DIR

# Install requirements
ADD ${SERVICE_NAME}/requirements /requirements
RUN pip install -r /requirements

# Install RadDeployLib
ADD ./RadDeployLib $DEPENDENCIES_DIR/RadDeployLib
RUN pip install $DEPENDENCIES_DIR/RadDeployLib

# Copy code
COPY $SERVICE_NAME $APP_DIR

# Go to exec space
WORKDIR $APP_DIR/src

ENTRYPOINT ["/usr/local/bin/python3", "main.py"]
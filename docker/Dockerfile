FROM --platform=linux/amd64 python:3.11
RUN apt update
RUN apt install -yq libpq-dev postgresql-client
RUN pip install --upgrade pip
RUN pip install sinker
RUN useradd sinker
ARG WORKDIR=/sinker
RUN mkdir $WORKDIR
RUN chown sinker:sinker $WORKDIR
USER sinker
WORKDIR $WORKDIR
COPY --chown=sinker:sinker cmd.sh ./
COPY --chown=sinker:sinker views_to_indices ./views_to_indices
CMD ["./cmd.sh"]

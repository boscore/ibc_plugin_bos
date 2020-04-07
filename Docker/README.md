# Run in docker

Simple and fast setup of BOSCore on Docker is also available.

## Install Dependencies

- [Docker](https://docs.docker.com) Docker 17.05 or higher is required
- [docker-compose](https://docs.docker.com/compose/) version >= 1.10.0

## Docker Requirement

- At least 7GB RAM (Docker -> Preferences -> Advanced -> Memory -> 7GB or above)
- If the build below fails, make sure you've adjusted Docker Memory settings and try again.

## Build BOSCore image

```bash
git clone https://github.com/boscore/ibc_plugin_bos.git --recursive 
cd bos/Docker
docker build -t winlin/ibc_plugin_bos:ibc-v4.0.0 --build-arg branch=ibc-v4.0.0   --build-arg symbol=BOS --no-cache .
```

## Start nodeos docker container only

```bash
docker run --name nodeos -p 8888:8888 -p 9876:9876 -t boscore/ibc_plugin_bos:ibc-v4.0.0 nodeosd.sh -e --http-alias=nodeos:8888 --http-alias=127.0.0.1:8888 --http-alias=localhost:8888 arg1 arg2
```

By default, all data is persisted in a docker volume. It can be deleted if the data is outdated or corrupted:

```bash
$ docker inspect --format '{{ range .Mounts }}{{ .Name }} {{ end }}' nodeos
fdc265730a4f697346fa8b078c176e315b959e79365fc9cbd11f090ea0cb5cbc
$ docker volume rm fdc265730a4f697346fa8b078c176e315b959e79365fc9cbd11f090ea0cb5cbc
```

Alternately, you can directly mount host directory into the container

```bash
docker run --name nodeos -v /path-to-data-dir:/opt/eosio/bin/data-dir -p 8888:8888 -p 9876:9876 -t boscore/ibc_plugin_bos:ibc-v4.0.0 nodeosd.sh -e --http-alias=nodeos:8888 --http-alias=127.0.0.1:8888 --http-alias=localhost:8888 arg1 arg2
```

### Docker Hub

Docker Hub image available from [docker hub](https://hub.docker.com/r/boscore/bos/).
Create a new `docker-compose.yaml` file with the content below

```bash
version: "3"

services:
  nodeosd:
    image: boscore/ibc_plugin_bos:ibc-v4.0.0
    command: /opt/eosio/bin/nodeosd.sh --data-dir /opt/eosio/bin/data-dir -e --http-alias=nodeosd:8888 --http-alias=127.0.0.1:8888 --http-alias=localhost:8888
    hostname: nodeosd
    ports:
      - 8888:8888
      - 9876:9876
    expose:
      - "8888"
    volumes:
      - nodeos-data-volume:/opt/eosio/bin/data-dir

volumes:
  nodeos-data-volume:

```

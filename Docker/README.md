# Run in docker

Simple and fast setup of ibc_plugin_bos on Docker is also available.

## Install Dependencies

- [Docker](https://docs.docker.com) Docker 17.05 or higher is required
- [docker-compose](https://docs.docker.com/compose/) version >= 1.10.0

## Docker Requirement

- At least 7GB RAM (Docker -> Preferences -> Advanced -> Memory -> 7GB or above)
- If the build below fails, make sure you've adjusted Docker Memory settings and try again.

## Build ibc_plugin_bos image

```bash
git clone https://github.com/boscore/ibc_plugin_bos.git --recursive  --depth 1
cd ibc_plugin_bos/Docker
docker build . -t boscore/ibc_plugin_bos  
```

The above will build off the most recent commit to the master branch by default. If you would like to target a specific branch/tag, you may use a build argument. For example, if you wished to generate a docker image based off of the ibc-v1.0.5 tag, you could do the following:

```bash
docker build -t boscore/ibc_plugin_bos:ibc-v1.0.5 --build-arg branch=ibc-v1.0.5 .

```

By default, the symbol in eosio.system is set to BOSCORE. You can override this using the symbol argument while building the docker image.

```bash
docker build -t boscore/ibc_plugin_bos --build-arg symbol=<symbol> .
```

### Docker Hub

Use docker image directly.

```
docker pull boscore/ibc_plugin_bos:ibc-v1.0.5
```




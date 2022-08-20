# PySpark Workspace

## Prerequisites

* JDK
* Scala

``` shell
pip install -r requirements.txt
```

``` shell
docker-compose up -d
```

## Environment variables

Set the environment variables for running locally in `util.py`, using
the bridge network.

Use an `.env` file for docker-compose.

## Local file hacks

```shell
ln -s $(pwd)/data /tmp/data
```


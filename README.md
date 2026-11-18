Gletscher
=========

Google Cloud Storage Backup Tool

![Build Status](https://github.com/pmoor/gletscher/actions/workflows/maven.yml/badge.svg)

Prerequisites
-------------

* Java 21 or higher
* [git](https://git-scm.com/downloads)
* [Maven](https://maven.apache.org/download.cgi)
* A [Google Cloud Storage](https://cloud.google.com/storage/) bucket with "Storage Object Creator" role service account.

Quick Start
-----------

Get the sources:
```
$ git clone https://github.com/pmoor/gletscher.git
$ cd gletscher/
```

Compile the latest version:
```
$ mvn verify
```

Try running the command:
```
$ ./target/gletscher help
```

Create your first backup config file by copying the sample and adjusting the relevant options:
```
$ cp sample-config.yaml config.yaml
$ ${EDITOR} config.yaml
```

Take a backup:
```
$ ./target/gletscher backup -c config.yaml
```

To restore the latest backup:
```
$ ./target/gletscher restore -c config.yaml "My Restore Directory"
```

JVM arguments can be passed via `GLETSCHER_JAVA_OPTS`:
```
$ GLETSCHER_JAVA_OPTS="-Xmx4g" ./target/gletscher backup -c config.yaml
```

Gletscher
=========

Google Cloud Storage Backup Tool

[![Build Status](https://travis-ci.org/pmoor/gletscher.svg?branch=master)](https://travis-ci.org/pmoor/gletscher)

Prerequisites
-------------

* [JRE 1.8 or higher](https://java.com/download/)
* [Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html)
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
$ java -jar target/gletscher-*-jar-with-dependencies.jar \
    help
```

Create your first backup config file by copying the sample and adjusting the relevant options:
```
$ cp sample-config.yaml config.yaml
$ ${EDITOR} config.yaml
```

Take a backup:
```
$ java -jar target/gletscher-*-jar-with-dependencies.jar \
    backup -c config.yaml
```

To restore the latest backup:
```
$ java -jar target/gletscher-*-jar-with-dependencies.jar \
    backup -c config.yaml "My Restore Directory"
```

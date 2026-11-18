#!/bin/sh
exec java ${GLETSCHER_JAVA_OPTS:-} -jar "$0" "$@"

#!/bin/bash

set -e
USERS=`echo $CLIENT_KEYS | sed -e "s/:[^=]*=/=/g" | sed -e "s/;/\n/g"` envsubst < shiro.ini.template > run/shiro.ini
fuseki-server
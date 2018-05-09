#!/bin/sh
set -e
CONNECTOR_VERSION=$(grep 'connectorVersion=' gradle.properties)
BASE_VERSION=$(echo ${CONNECTOR_VERSION}  | sed 's/connectorVersion=//' | sed 's/-.*//')
VERSION="${BASE_VERSION}-$(git rev-list HEAD --count).$(git rev-parse --short HEAD)-$(date +%s)"
case "${CONNECTOR_VERSION}" in
*SNAPSHOT*) echo "${VERSION}-SNAPSHOT" ;;
*         ) echo "${VERSION}" ;;
esac
#!/bin/zsh

while :; do
if [ -e /tmp/fire ] ; then ; curl -X POST -H "Authorization: Bearer ${CLOUDBIT_TOKEN}" -H "Accept: application/vnd.littlebits.v2+json" 'https://api-http.littlebitscloud.cc/devices/${DEVICE_ID}/output' && rm /tmp/fire ; fi
sleep 5
done

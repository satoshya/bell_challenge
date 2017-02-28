#!/bin/zsh

curl -i -XGET -H "Authorization: Bearer ${CLOUDBIT_TOKEN}" -H "Accept: application/vnd.littlebits.v2+json" 'https://api-http.littlebitscloud.cc/devices/${DEVICE_ID}/input' >| /tmp/stream_data

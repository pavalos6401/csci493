#!/usr/bin/env sh

curl -H 'Content-Type: application/json' https://data.sagecontinuum.org/api/v1/query -d '
{
  "start": "-10s",
  "filter": {
    "sensor": "bme680"
  }
}
' > fetched.data

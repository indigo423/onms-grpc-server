#!/bin/bash -e

get_key() {
  echo "$1" | cut -d_ -f2- | tr '[:upper:]' '[:lower:]' | tr _ .
}

IFS=$'\n'
PRODUCER=()
CONSUMER=()
for VAR in $(env)
do
  env_var=$(echo "$VAR" | cut -d= -f1)
  if [[ $env_var =~ ^CONSUMER_ ]]; then
    echo "[configuring consumer] processing $env_var"
    key=$(get_key $env_var)
    echo "[configuring consumer] key: $key"
    val=${!env_var}
    echo "[configuring consumer] value: $val"
    CONSUMER+=(-consumer-cfg "$key=$val")
  fi
  if [[ $env_var =~ ^PRODUCER_ ]]; then
    echo "[configuring producer] processing $env_var"
    key=$(get_key $env_var)
    echo "[configuring producer] key: $key"
    val=${!env_var}
    echo "[configuring producer] value: $val"
    PRODUCER+=(-producer-cfg "$key=$val")
  fi
done

OPTIONS=()
if [ ! -z "${PORT}" ]; then
  OPTIONS+=(-port "${PORT}")
fi
if [ ! -z "${HTTP_PORT}" ]; then
  OPTIONS+=(-http-port "${HTTP_PORT}")
fi
if [ ! -z "${BOOTSTRAP_SERVER}" ]; then
  OPTIONS+=(-bootstrap "${BOOTSTRAP_SERVER}")
fi
if [ "${TLS_ENABLED}" == "true" ]; then
  OPTIONS+=(-tls-enabled)
fi
if [ ! -z "${TLS_CERT}" ]; then
  OPTIONS+=(-tls-cert ${TLS_CERT})
fi
if [ ! -z "${TLS_KEY}" ]; then
  OPTIONS+=(-tls-key ${TLS_KEY})
fi
if [ ! -z "${INSTANCE_ID}" ]; then
  OPTIONS+=(-instance-id "${INSTANCE_ID}")
fi
if [ ! -z "${MAX_BUFFER_SIZE}" ]; then
  OPTIONS+=(-max-buffer-size "${MAX_BUFFER_SIZE}")
fi

exec /bin/onms-grpc-server ${OPTIONS[@]} ${PRODUCER[@]} ${CONSUMER[@]}

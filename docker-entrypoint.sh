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
    echo "[configuring producer] '$key'='$val'"
    PRODUCDER+=(-producer-cfg "$key=$val")
  fi
done

OPTIONS=()
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

exec /onms-grpc-server \
  -port "${PORT-8990}" \
  -bootstrap "${BOOTSTRAP_SERVER-localhost:9092}" \
  ${OPTIONS[@]} \
  ${CONSUMER[@]} \
  ${PRODUCDER[@]}

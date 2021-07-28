#!/bin/bash

FOXTROT_CONFIG_PATH="/config/docker.yml"

if [ -z "${CONFIG_PATH}" ]; then
  :
else
  FOXTROT_CONFIG_PATH=${CONFIG_PATH}
fi
echo "Using config path: ${FOXTROT_CONFIG_PATH}"


if [ -z "${STARTUP_DELAY}" ]; then
  echo "No delay specified. To wait for stores to initialize, please provide a delay in the STARTUP_DELAY variable"
else
  echo "Sleeping for ${STARTUP_DELAY} seconds"
  sleep ${STARTUP_DELAY}
fi

if [ -z "${SKIP_INITIALIZATION}" ]; then
  timeout 10 timeout 10 java -jar server.jar initialize "${FOXTROT_CONFIG_PATH}"
else
  echo "Skipping initialization for foxtrot data stores"
fi

CMD="java -Dfile.encoding=utf-8 -XX:+${GC_ALGO-UseG1GC} -Xms${JAVA_PROCESS_MIN_HEAP-1g} -Xmx${JAVA_PROCESS_MAX_HEAP-1g} ${JAVA_OPTS} -jar server.jar server ${FOXTROT_CONFIG_PATH}"
echo "Executing server command: ${CMD}"
${CMD}
#!/bin/bash

FOXTROT_CONFIG_FILE=/config/local.yml

if [ -z "${CONFIG_PATH}" ]; then
    echo "No CONFIG_PATH defined. We shall be using default config from ${FOXTROT_CONFIG_FILE}"
else
    if [ -f ${FOXTROT_CONFIG_FILE} ]; then
        FOXTROT_CONFIG_FILE=${CONFIG_PATH}
    else
        echo "Defined CONFIG_PATH (${CONFIG_PATH}) doesn ot look like a proper file. Using default: ${FOXTROT_CONFIG_FILE}"
    fi
fi

if [ -z "${INIT_SLEEP}" ]; then
    echo "No initial sleep specified.. Foxtrot will start up immediately"
else
    echo -e "Init sleep of ${INIT_SLEEP} seconds specified. Waiting..."
    sleep ${INIT_SLEEP}
    echo "done"
fi

INIT_COMMAND="java -jar server.jar initialize ${FOXTROT_CONFIG_FILE}"

EXEC_CMD="java -Dfile.encoding=utf-8 -XX:+${GC_ALGO-UseG1GC} -Xms${JAVA_PROCESS_MIN_HEAP-1g} -Xmx${JAVA_PROCESS_MAX_HEAP-1g} ${JAVA_OPTS} -jar server.jar server ${FOXTROT_CONFIG_FILE}"

if [ -z "${SKIP_INIT}" ]; then
    echo "Executing Init Command: ${INIT_COMMAND}"
    $INIT_COMMAND
else
    echo "Skipping Init as $SKIP_INIT was set"
fi
echo "Starting foxtrot with command line: ${EXEC_CMD}"
$EXEC_CMD

FROM ubuntu:16.04
MAINTAINER Santanu Sinha <santanu DOT sinha [at] gmail.com>

RUN apt-get clean && apt-get update && apt-get install -y --no-install-recommends software-properties-common
RUN add-apt-repository ppa:openjdk-r/ppa && apt-get update
RUN apt-get install -y --no-install-recommends openjdk-8-jdk ca-certificates && apt-get install -y --no-install-recommends ca-certificates-java bash curl tzdata iproute2 zip unzip wget


EXPOSE 17000
EXPOSE 17001
EXPOSE 5701

VOLUME /var/log/foxtrot-server

ADD config/docker.yml docker.yml
ADD foxtrot-server/target/foxtrot-server*.jar server.jar
ADD scripts/local_es_setup.sh local_es_setup.sh

CMD sh -c "sleep 15 ; java -jar server.jar initialize docker.yml || true ;  java -Dfile.encoding=utf-8 -XX:+${GC_ALGO-UseG1GC} -Xms${JAVA_PROCESS_MIN_HEAP-1g} -Xmx${JAVA_PROCESS_MAX_HEAP-1g} ${JAVA_OPTS} -jar server.jar server docker.yml"


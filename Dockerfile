FROM ubuntu:14.04
MAINTAINER Nitish Goyal <nitishgoyal13 [at] gmail.com>


RUN \
  apt-get clean && apt-get update && apt-get install -y --no-install-recommends software-properties-common \
  && add-apt-repository ppa:webupd8team/java \
  && gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 \
  && apt-get update \
  && echo debconf shared/accepted-oracle-license-v1-1 select true |  debconf-set-selections \
  && echo debconf shared/accepted-oracle-license-v1-1 seen true |  debconf-set-selections \
  && apt-get install -y --no-install-recommends oracle-java8-installer \
  && apt-get install -y --no-install-recommends curl

EXPOSE 17000
EXPOSE 17001
EXPOSE 5701

VOLUME /var/log/foxtrot

ENV JAR_FILE foxtrot.jar

ADD config/docker.yml docker.yml
ADD foxtrot-server/target/foxtrot*.jar server.jar
ADD scripts/local_es_setup.sh local_es_setup.sh

CMD sh -exc "java -jar -Duser.timezone=Asia/Kolkata ${JAVA_OPTS} -Xms${JAVA_PROCESS_MIN_HEAP-512m} -Xmx${JAVA_PROCESS_MAX_HEAP-512m} ${JAR_FILE} server /rosey/config.yml"

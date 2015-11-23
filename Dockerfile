FROM ubuntu:14.04
MAINTAINER Shashank <shashank.g [at] olacabs.com>


RUN \
  apt-get install -y --no-install-recommends software-properties-common \
  && add-apt-repository ppa:webupd8team/java \
  && gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 \
  && apt-get update \
  && echo debconf shared/accepted-oracle-license-v1-1 select true |  debconf-set-selections \
  && echo debconf shared/accepted-oracle-license-v1-1 seen true |  debconf-set-selections \
  && apt-get install -y --no-install-recommends oracle-java8-installer

EXPOSE 17000
EXPOSE 17001

VOLUME /var/log/foxtrot-server

ADD config/docker.yml docker.yml
ADD foxtrot-server/target/foxtrot*.jar server.jar

CMD sh -c "sleep 10; java -jar -Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl server.jar server docker.yml"


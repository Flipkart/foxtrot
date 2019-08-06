FROM docker.phonepe.com:5000/pp-ops-xenial:0.6
EXPOSE 17000
EXPOSE 17001
EXPOSE 5701

VOLUME /var/log/foxtrot

ENV JAR_FILE foxtrot.jar

ADD foxtrot-server/target/foxtrot*.jar ${JAR_FILE}
CMD sh -exc "java -jar -Duser.timezone=Asia/Kolkata ${JAVA_OPTS} -Xms${JAVA_PROCESS_MIN_HEAP-512m} -Xmx${JAVA_PROCESS_MAX_HEAP-512m} ${JAR_FILE} server /rosey/config.yml"

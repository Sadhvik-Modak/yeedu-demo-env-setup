# syntax=docker/dockerfile:1
FROM apache/hive:4.0.0 AS hive-base

# apache/hive:4.0.0 ships a JRE only (no javac), so compile the custom
# HiveServer2 authenticator in a real JDK image against Hive's own
# hive-service jar, then copy just the built jar into the runtime image.
FROM eclipse-temurin:8-jdk AS auth-builder
COPY --from=hive-base /opt/hive/lib/hive-service-4.0.0.jar /opt/hive/lib/hive-service-rpc-4.0.0.jar /libs/
WORKDIR /src
COPY auth/SimplePasswdAuthenticator.java com/yeedu/demo/auth/SimplePasswdAuthenticator.java
RUN mkdir /out \
    && javac -cp "/libs/*" -d /out com/yeedu/demo/auth/SimplePasswdAuthenticator.java \
    && cd /out && jar cf /simple-passwd-authenticator.jar com

FROM apache/hive:4.0.0
USER root

ARG POSTGRES_JDBC_VERSION=42.7.4
ADD https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar /opt/hive/lib/postgresql-jdbc.jar
COPY --from=auth-builder /simple-passwd-authenticator.jar /opt/hive/lib/simple-passwd-authenticator.jar
# S3A connector (for MinIO): the image already ships these under Hadoop's
# optional tools/lib, just not on Hive's classpath by default.
RUN ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.3.6.jar /opt/hive/lib/hadoop-aws-3.3.6.jar \
    && ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.12.367.jar /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

RUN chown hive:hive /opt/hive/lib/postgresql-jdbc.jar /opt/hive/lib/simple-passwd-authenticator.jar

# HiveServer2 requires its scratch dir to be writable by all (HDFS-style
# permission check even in local mode); pre-create it so it doesn't end up
# with a restrictive default umask.
RUN mkdir -p /tmp/hive && chmod 1777 /tmp/hive

USER hive

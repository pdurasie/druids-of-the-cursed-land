FROM hseeberger/scala-sbt:17.0.2_1.6.2_3.1.1 as scala_builder

WORKDIR /app

COPY . .

RUN sbt pack

FROM anapsix/alpine-java:8u131b11_jdk

COPY --from=scala_builder /app/target/pack /srv/myapp

USER nobody
WORKDIR /srv/myapp
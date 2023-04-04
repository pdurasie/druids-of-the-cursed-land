FROM hseeberger/scala-sbt:11.0.6_1.3.10_2.13.1 as scala_builder

WORKDIR /app

COPY . .

RUN sbt pack

FROM anapsix/alpine-java:8u131b11_jdk

COPY --from=scala_builder /app/target/pack /srv/myapp

USER nobody
WORKDIR /srv/myapp
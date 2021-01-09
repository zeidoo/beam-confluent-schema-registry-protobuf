# Purpose

Demo project to show how to use Apache Beam with the Confluent Schema Registry and Protobuf.

# Organization

* The sample kafka producer is under com.zeidoo.producer.kafka.
* A simple consumer that uses Apache Beam functions to deserialize under com.zeidoo.consumer.simple.
* A consumer that uses Apache Beam's DeserializerProvider is under com.zeidoo.consumer.provider.

# Simple Consumer

1. Create a CachedSchemaRegistryClient.
1. Configure KafkaIO to return use a ByteArrayDeserializer. `withValueDeserializer(ByteArrayDeserializer.class)`
1. Use the CachedSchemaRegistryClient to create a coder and configure the pipeline to use it.
1. Create a KafkaProtobufDeserializer and deserialize the byte array to a DynamicMessage.

# DeserializerProvider Consumer

1. Same ideas as the Simple Consumer but all in one place.
1. Without the inner deserializer class `InnerDeserializer`, the code doesn't work.
1. KafkaIO can now use DynamicMessage instead of byte array.

# Notes

* You can configure the KafkaProtobufDeserializer to return concrete implementations (ie: the protoc generated classes) by reading [this](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html).
* The `SchemaRegistryProtobufDeserializer` is a copy paste of the `ConfluentSchemaRegistryDeserializerProvider` class from the Beam project. It only works for Avro.
* `DeserializerProvider<T>` is not public so `SchemaRegistryProtobufDeserializer` needs to be in `package org.apache.beam.sdk.io.kafka;`

# Docker
You can start up Zookeper, Kafka and the Confluent Schema Registry with these commands:


```
docker run -d \
-p 2181:2181 \
--net=mynet \
--name=zookeeper \
-e ALLOW_ANONYMOUS_LOGIN=yes \
-e ZOOKEEPER_CLIENT_PORT=2181 \
confluentinc/cp-zookeeper:5.5.3

docker run -d \
--net=mynet \
--name=kafka \
-p 9092:9092 \
-e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
-e KAFKA_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://0.0.0.0:9092 \
-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092 \
-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
-e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
confluentinc/cp-kafka:5.5.3


docker run -d \
--net=mynet \
--name=schema-registry \
-p 8081:8081 \
-e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:2181 \
-e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
-e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
confluentinc/cp-schema-registry:5.5.3
```
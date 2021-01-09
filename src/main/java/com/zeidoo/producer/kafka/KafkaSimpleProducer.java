package com.zeidoo.producer.kafka;

import com.zeidoo.protobuf.InnerMessageOuterClass;
import com.zeidoo.protobuf.OuterMessageOuterClass;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaSimpleProducer {

    // most code straight from the confluent website
    public static void main(final String[] args) throws IOException {

        String topic = "dummytopic";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);

        // Create topic if needed
        createTopic(topic, 1, 1, props);

        // Produce sample data
        var producer = new KafkaProducer<String, OuterMessageOuterClass.OuterMessage>(props);
        final Long numMessages = 10L;
        for (Long i = 0L; i < numMessages; i++) {
            String key = "key" + i;
            var record = OuterMessageOuterClass.OuterMessage.newBuilder()
                    .setOuterProp("Key is:" + key)
                    .setInner(InnerMessageOuterClass.InnerMessage.newBuilder().setInnerProp("Inner prop").build())
                    .build();

            System.out.printf("Producing record: %s\t%s%n", key, record);
            producer.send(new ProducerRecord<>(topic, key, record), (m, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                }
            });
        }

        producer.flush();
        System.out.printf("10 messages were produced to topic %s%n", topic);
        producer.close();
    }

    // Create topic in Confluent Cloud
    public static void createTopic(final String topic,
                                   final int partitions,
                                   final int replication,
                                   final Properties cloudConfig) {
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }
}

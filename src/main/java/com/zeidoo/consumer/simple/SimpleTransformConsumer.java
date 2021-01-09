package com.zeidoo.consumer.simple;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.DynamicProtoCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class SimpleTransformConsumer {

    private static String topic = "dummytopic";
    private static CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(Arrays.asList("http://localhost:8081"), 1000, Arrays.asList(new ProtobufSchemaProvider()), new HashMap<String, String>());

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.as(FlinkPipelineOptions.class).setRunner(FlinkRunner.class);

        DynamicProtoCoder dynamicProtoCoder = null;
        try {
            int id = client.getLatestSchemaMetadata(topic + "-value").getId();
            ProtobufSchema schema = (ProtobufSchema) client.getSchemaById(id);
            dynamicProtoCoder = DynamicProtoCoder.of(schema.toDescriptor());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        Pipeline p = Pipeline.create(options);
        p.apply(KafkaIO.<String, byte[]>read()
                .withStartReadTime(DateTime.now().minusDays(5).toInstant())
                .withBootstrapServers("localhost:9092")
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withoutMetadata()
        )
                .apply(MapElements.via(new ToProtobufMessageFn()))
                .setCoder(dynamicProtoCoder)
                .apply(MapElements.via(new ConsumeFn()));

        p.run().waitUntilFinish();
    }

    public static class ToProtobufMessageFn extends SimpleFunction<KV<String, byte[]>, DynamicMessage> {
        @Override
        public DynamicMessage apply(KV<String, byte[]> input) {
            var deserializer = new KafkaProtobufDeserializer(client);
            return (DynamicMessage) deserializer.deserialize(topic, input.getValue());
        }
    }

    public static class ConsumeFn extends SimpleFunction<Message, String> {
        @Override
        public String apply(Message input) {
            String s = input.toString();
            System.out.println(s);
            return s;
        }
    }
}

package com.zeidoo.consumer.provider;

import com.google.protobuf.DynamicMessage;
import com.zeidoo.protobuf.OuterMessageOuterClass;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.SchemaRegistryProtobufDeserializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;

public class DynamicMessageDecoderProviderConsumer {

    private static String topic = "dummytopic";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.as(FlinkPipelineOptions.class).setRunner(FlinkRunner.class);

        Pipeline p = Pipeline.create(options);
        p.apply(KafkaIO.<String, DynamicMessage>read()
                .withStartReadTime(DateTime.now().minusDays(5).toInstant())
                .withBootstrapServers("localhost:9092")
                .withTopic(topic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(SchemaRegistryProtobufDeserializer.of("http://localhost:8081", topic, DynamicMessage.class))
                .withoutMetadata()
        )
                .apply(MapElements.via(new ConsumeFn()));

        p.run().waitUntilFinish();
    }

    public static class ConsumeFn extends SimpleFunction<KV<String, DynamicMessage>, String> {
        @Override
        public String apply(KV<String, DynamicMessage> input) {
            String s = input.getValue().toString();
            System.out.println(s);
            return s;
        }
    }
}

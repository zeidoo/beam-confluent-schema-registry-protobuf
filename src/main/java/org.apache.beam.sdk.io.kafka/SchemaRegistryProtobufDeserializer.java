package org.apache.beam.sdk.io.kafka;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.zeidoo.protobuf.OuterMessageOuterClass;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.protobuf.DynamicProtoCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.KeyForBottom;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Experimental(Experimental.Kind.SOURCE_SINK)
public class SchemaRegistryProtobufDeserializer<T extends Message> implements DeserializerProvider<T> {

    @UnknownKeyFor
    @NonNull
    @Initialized
    private final SerializableFunction<Void, SchemaRegistryClient> schemaRegistryClientProviderFn;
    @UnknownKeyFor
    @NonNull
    @Initialized
    private final String schemaRegistryUrl;
    @Nullable
    @UnknownKeyFor
    @Initialized
    private final Integer version;

    @Nullable
    @UnknownKeyFor
    @Initialized
    private final String topic;

    @UnknownKeyFor
    @Nullable
    @Initialized
    private final Class<T> clazz;

    @VisibleForTesting
    SchemaRegistryProtobufDeserializer(@UnknownKeyFor @NonNull @Initialized SerializableFunction<Void, SchemaRegistryClient> schemaRegistryClientProviderFn, @UnknownKeyFor @NonNull @Initialized String schemaRegistryUrl, @Nullable @UnknownKeyFor @Initialized Integer version, String topic, @UnknownKeyFor @NonNull @Initialized Class<T> clazz) {
        Preconditions.checkArgument(schemaRegistryClientProviderFn != null, "You should provide a schemaRegistryClientProviderFn.");
        Preconditions.checkArgument(schemaRegistryUrl != null, "You should provide a schemaRegistryUrl.");
        this.schemaRegistryClientProviderFn = schemaRegistryClientProviderFn;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.version = version;
        this.topic = topic;
        this.clazz = clazz;
    }

    @UnknownKeyFor
    @NonNull
    @Initialized
    public static <T extends Message> SchemaRegistryProtobufDeserializer<T> of(@UnknownKeyFor @NonNull @Initialized String schemaRegistryUrl, String topic, @UnknownKeyFor @NonNull @Initialized Class<T> clazz) {
        return of(schemaRegistryUrl, null, topic, clazz);
    }

    @UnknownKeyFor
    @NonNull
    @Initialized
    public static <T extends Message> SchemaRegistryProtobufDeserializer<T> of(@UnknownKeyFor @NonNull @Initialized String schemaRegistryUrl, @Nullable @UnknownKeyFor @Initialized Integer version, String topic, @UnknownKeyFor @NonNull @Initialized Class<T> clazz) {
        return new SchemaRegistryProtobufDeserializer((input) -> new CachedSchemaRegistryClient(Arrays.asList(schemaRegistryUrl), 2147483647, Arrays.asList(new ProtobufSchemaProvider()), new HashMap<String, String>()), schemaRegistryUrl, version, topic, clazz);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Deserializer<T> getDeserializer(@UnknownKeyFor @NonNull @Initialized Map<@UnknownKeyFor @NonNull @Initialized String, @KeyForBottom @NonNull @Initialized ?> configs, @UnknownKeyFor @NonNull @Initialized boolean isKey) {
        return new InnerDeserializer(this.schemaRegistryUrl, this.clazz);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Coder<T> getCoder(@UnknownKeyFor @NonNull @Initialized CoderRegistry coderRegistry) {
        if(this.clazz != DynamicMessage.class)
            return ProtoCoder.of(this.clazz);

        DynamicProtoCoder dynamicProtoCoder = null;
        try {
            int id = getSchemaRegistryClient().getLatestSchemaMetadata(topic + "-value").getId();
            ProtobufSchema schema = (ProtobufSchema) getSchemaRegistryClient().getSchemaById(id);
            dynamicProtoCoder = DynamicProtoCoder.of(schema.toDescriptor());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        return (Coder<T>) dynamicProtoCoder;
    }

    @UnknownKeyFor
    @NonNull
    @Initialized
    private SchemaRegistryClient getSchemaRegistryClient() {
        return this.schemaRegistryClientProviderFn.apply(null);
    }

    private class InnerDeserializer implements Deserializer<T> {
        private String schemaRegistryUrl;
        private final Class<T> clazz;
        private KafkaProtobufDeserializer deserializer;

        public InnerDeserializer(@UnknownKeyFor @NonNull @Initialized String schemaRegistryUrl, Class<T> clazz) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.clazz = clazz;
        }

        @Override

        public T deserialize(String s, byte[] bytes) {
            if (this.deserializer == null) {
                deserializer = new KafkaProtobufDeserializer(getSchemaRegistryClient());

                if (this.clazz != DynamicMessage.class) {
                    Properties props = new Properties();
                    props.put("schema.registry.url", this.schemaRegistryUrl);
                    props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, this.clazz.getName());
                    deserializer.configure(props, false);
                }
            }
            return (T) deserializer.deserialize(null, bytes);
        }
    }
}

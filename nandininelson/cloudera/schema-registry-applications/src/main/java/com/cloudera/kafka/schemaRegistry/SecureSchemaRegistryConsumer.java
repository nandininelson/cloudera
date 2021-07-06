package com.cloudera.kafka.schemaRegistry;

// Section: Imports

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

public class SecureSchemaRegistryConsumer {

    public static final Logger logger = LoggerFactory.getLogger(SecureSchemaRegistryConsumer.class.getName());

    public static final String trustStorePassword = "*******";
    public static final String trustStorePath = "/Users/nandini/truststore.jks";
    public static final String USERNAME = "user";
    public static final String PASSWORD = "pass";
    public static final String SECURITY_PROTOCOL = "SASL_SSL";
    public static final String SASL_MECHANISM = "GSSAPI";
    public static final String bootstrapServers = "Kafka1:9092,Kafka2:9092,Kafka3:9092";
    public static final String topic = "device";

    // Schema Registry Configs (Schema Registry URL and Schema Name)
    public static final String SCHEMA_REGISTRY_URL = "https://KnoxGateway:8443/gateway/cdp-proxy-api/schema-registry/api/v1";
    public static final String SCHEMA_NAME = "device";
    public static final String JAAS_CONF = "/Users/nandini/jaas.conf";
    public static final String SERVICE_NAME = "kafka";

    public static final String groupId = "testSRGroup";

    public static void main(String[] args)  throws Exception {

        System.setProperty("java.security.auth.login.config", JAAS_CONF);

        // create consumer configs
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.put(AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);

        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Security Configs
        config.put("security.protocol", SECURITY_PROTOCOL);
        config.put("sasl.mechanism", SASL_MECHANISM);
        config.put("sasl.kerberos.service.name", SERVICE_NAME);
        // Add if TLS is Enabled
        // Find info here - https://ClouderManager:7183/api/v17/cm/config
        config.put("ssl.truststore.location", trustStorePath);
        config.put("ssl.truststore.password", trustStorePassword);
        config.put(SchemaRegistryClient.Configuration.AUTH_USERNAME.name(), USERNAME);
        config.put(SchemaRegistryClient.Configuration.AUTH_PASSWORD.name(), PASSWORD);

        // Create a SR Client
        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);

        // Add if TLS SSL Enabled
        HashMap<String, String> sslMap = new HashMap<>();
        sslMap.put("protocol", "SSL");
        sslMap.put("trustStoreType", "JKS");
        sslMap.put("trustStorePath", trustStorePath);
        sslMap.put("trustStorePassword", trustStorePassword);
        config.put("schema.registry.client.ssl", sslMap);

        // Configurations for SR
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        // Get a specific version of Schema from SR
        Map<String, Integer> readerVersions = new HashMap<>();
        readerVersions.put(SCHEMA_NAME, 1/*SCHEMA_VERSION*/);
        config.put("schemaregistry.reader.schema.versions", readerVersions);

        // Create consumer
        KafkaConsumer<Long, Object> consumer = new KafkaConsumer<Long, Object>(config);
        // Subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        // Section 5: poll for new data
        try {
            while (true) {
                ConsumerRecords<Long, Object> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Long, Object> record : records) {
                    System.out.print(
                            "Key: " + record.key() +
                            ", Value: " + record.value() +
                            ", Partition: " + record.partition() +
                            ", Offset:" + record.offset() +
                            ", Schema Version:" + readerVersions.get(SCHEMA_NAME)
                    );

                    // Section 5.2 verify the record
                    GenericRecord avroRecord = (GenericRecord)record.value();
                    System.out.println(", Value: { xid = " + avroRecord.get("xid") +
                            " name = " + avroRecord.get("name") +
                            " version = " + avroRecord.get("version")+
                            " timestamp = " + avroRecord.get("timestamp") + " }"
                                );
                }
                // commit offsets
                consumer.commitSync();
            }
        }
        finally{
            // Close consumer
            consumer.close();
        }


    }
}

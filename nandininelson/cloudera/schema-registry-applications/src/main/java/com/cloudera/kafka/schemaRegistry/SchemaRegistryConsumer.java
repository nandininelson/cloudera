package com.cloudera.kafka.schemaRegistry;

// Section: Imports

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SchemaRegistryConsumer {

    public static final Logger logger = LoggerFactory.getLogger(SchemaRegistryConsumer.class.getName());
    public static void main(String[] args)  throws Exception {

        // Get arguments
        final String bootstrapServers = "Kafka1:9092,Kafka2:9092,Kafka3:9092";
        final String topic = "testSRDemo";
        final String groupId = "testSRDemo";

        // Schema Registry Configs
        final String SCHEMA_REGISTRY_URL = "http://SchemaRegistry:7788/api/v1";
        final String SCHEMA_NAME = topic;

        // Section 2: create consumer configs
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        // Configurations for SR
        props.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        // Get a specific version of Schema from SR
        Map<String, Integer> readerVersions = new HashMap<>();
        readerVersions.put(SCHEMA_NAME, 1/*SCHEMA_VERSION*/);
        props.put("schemaregistry.reader.schema.versions", readerVersions);

        // Create consumer
        KafkaConsumer<Long, Object> consumer = new KafkaConsumer<Long, Object>(props);
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

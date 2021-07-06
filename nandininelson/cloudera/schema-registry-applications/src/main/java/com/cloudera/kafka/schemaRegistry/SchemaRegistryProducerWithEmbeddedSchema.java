package com.cloudera.kafka.schemaRegistry;

// Section: Imports

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.AbstractAvroSnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

public class SchemaRegistryProducerWithEmbeddedSchema {

    public static final Logger logger = LoggerFactory.getLogger(SchemaRegistryProducerWithEmbeddedSchema.class.getName());
    public static void main(String[] args) throws Exception {

        // Get arguments
        final String bootstrapServers = "Kafka1:9092,Kafka2:9092,Kafka3:9092";
        final String topic = "testSRDemo";

        // Schema Registry Configs (Schema Registry URL and Schema Name)
        final String SCHEMA_REGISTRY_URL = "http://SchemaRegistry:7788/api/v1";
        final String SCHEMA_NAME = topic;


        // Create Producer props
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);

        // Configurations for SR
        props.setProperty(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);

        // Create a SR Client
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000);

        // Create schema
        Schema.Parser parser = new Schema.Parser();

        String userSchema = "{\n" +
                "   \"type\" : \"record\",\n" +
                "   \"namespace\" : \"com.nandini\",\n" +
                "   \"name\" : \"device\",\n" +
                "   \"fields\" : [\n" +
                "      { \"name\" : \"xid\" , \"type\" : \"long\" },\n" +
                "      { \"name\" : \"name\" , \"type\" : \"string\" },\n" +
                "      { \"name\" : \"version\" , \"type\" : \"int\" },\n" +
                "      { \"name\" : \"timestamp\" , \"type\" : \"int\" }\n" +
                "   ]\n" +
                "}";
        Schema schema = parser.parse(userSchema);
        // Create the producer
        KafkaProducer<Long, Object> producer = new KafkaProducer<Long, Object>(props);
        // Section 5: create a producer record and send data asynchronously at an interval of 1 sec
        try{

            while(true){
                // Create a Generic record
                Long timestamp = System.currentTimeMillis();
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("xid", 12345L);
                avroRecord.put("name", "device12345");
                avroRecord.put("version", 0);
                avroRecord.put("timestamp", timestamp);
                // Create a producer record
                ProducerRecord<Long, Object > record = new ProducerRecord<Long, Object >(topic, timestamp, avroRecord);
                // Sends data asynchronously
               producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            System.out.println("Received new metadata." +
                                    "Topic:" + recordMetadata.topic()  +
                                    ", Partition: " + recordMetadata.partition()  +
                                    ", Offset: " + recordMetadata.offset() +
                                    ", Timestamp: " + recordMetadata.timestamp() +
                                    ", Record: " + record.value());
                        } else {
                            System.err.println("Error while producing" + e);
                        }
                    }
                });
                // sleep for 1 sec
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            // Flushes and close producer
            producer.flush();
            producer.close();
        }
    }
}

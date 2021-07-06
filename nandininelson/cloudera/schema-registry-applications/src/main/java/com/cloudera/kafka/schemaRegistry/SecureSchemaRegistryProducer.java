package com.cloudera.kafka.schemaRegistry;

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
import java.util.concurrent.TimeUnit;

import static com.hortonworks.registries.schemaregistry.serdes.avro.SerDesProtocolHandlerRegistry.METADATA_ID_VERSION_PROTOCOL;

public class SecureSchemaRegistryProducer {
    public static final Logger logger = LoggerFactory.getLogger(SecureSchemaRegistryProducer.class.getName());
    public static final String trustStorePassword = "******";
    public static final String trustStorePath = "/Users/nandini/truststore.jks";
    public static final String USERNAME = "user";
    public static final String PASSWORD = "pass";
    public static final String SECURITY_PROTOCOL = "SASL_SSL";
    public static final String SASL_MECHANISM = "GSSAPI";
    public static final String bootstrapServers = "Kafka1:9092,Kafka2:9092,Kafka3:9092";//configUtil.getProperties("bootstrap-server");
    public static final String topic = "device";
    // Schema Registry Configs (Schema Registry URL and Schema Name)
    public static final String SCHEMA_REGISTRY_URL = "https://KnoxGateway:8443/gateway/cdp-proxy-api/schema-registry/api/v1";
    public static final String SCHEMA_NAME = "device";
    public static final String JAAS_CONF = "/Users/nandini/jaas.conf";
    public static final String SERVICE_NAME = "kafka";

    public static void main(String[] args) throws Exception {

        System.setProperty("java.security.auth.login.config", JAAS_CONF);
        //String propertiesFile = "/Users/nandini/producer_device.properties";

        // Get arguments

        // Configurations
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), SCHEMA_REGISTRY_URL);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        config.put(AbstractAvroSnapshotSerializer.SERDES_PROTOCOL_VERSION, METADATA_ID_VERSION_PROTOCOL);

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


        // Add if TLS SSL Enabled
        HashMap<String, String> sslMap = new HashMap<>();
        sslMap.put("protocol", "SSL");
        sslMap.put("trustStoreType", "JKS");
        sslMap.put("trustStorePath", trustStorePath);
        sslMap.put("trustStorePassword", trustStorePassword);
        config.put("schema.registry.client.ssl", sslMap);

        // Create a SR Client
        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);

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
        KafkaProducer<Long, Object> producer = new KafkaProducer<Long, Object>(config);

        // Create a producer record and send data asynchronously at an interval of 1 sec
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
                //sleep for 1 sec
                Thread.sleep(1000);
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            // Flushes and close producer
            producer.flush();
            producer.close(10000, TimeUnit.MILLISECONDS);
        }
    }
}

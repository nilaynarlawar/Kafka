package Demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        final ProducerRecord<String,String> record = new ProducerRecord<String, String>("FirstTopic", "Hello World111");

        //send the data - asynchronous
        try {
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //exceutes every time a record succcessfully sent or an exception is thrown
                    if(e == null) {
                        // record was sent successfully
                        log.info("Recieved new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                        System.out.println("Recieved new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
            producer.flush();
            producer.close();
        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
    }
}

package Demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServer = "127.0.0.1:9092";
        final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        for(int i = 0 ; i <10; i++)
        {
         String topic = "FirstTopic";
         String value = "Hello World" + Integer.toString(i);
         String key = "id_" + Integer.toString(i);
         ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic, key, value);

         log.info("Key" + key);
            //id_0 is going to partition 1
            //id_1 is going to partition 0
            //id_2 is going to partition 2
            //id_3 is going to partition 0
            //id_4 is going to partition 2
            //id_5 is going to partition 2
            //id_6 is going to partition 0
            //id_7 is going to partition 2
            //id_8 is going to partition 1
            //id_9 is going to partition 2


        //send the data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //exceutes every time a record succcessfully sent or an exception is thrown
                    if (e == null) {
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
            }).get();  //block the .send() to make it synchronous - don't do this in production!!!!
        }
            producer.flush();
            producer.close();
    }
}

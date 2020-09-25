package Demo1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo1 {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("FirstTopic", "Hello World");

        //send the data - asynchronous
        try {
            producer.send(record);
            producer.flush();
            producer.close();
        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
    }
}

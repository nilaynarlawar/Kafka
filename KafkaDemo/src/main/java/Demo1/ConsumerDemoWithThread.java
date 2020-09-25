package Demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }

    private ConsumerDemoWithThread() {
    }

    private void run() {
        String bootstrapserver = "127.0.0.1:9092";
        String groupId = "myThirdApplication";
        String topic = "FirstTopic";
        CountDownLatch latch = new CountDownLatch(1);
        Logger log = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        log.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, topic ,bootstrapserver, groupId);

        Thread mythread = new Thread(myConsumerRunnable);
        mythread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            log.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch(InterruptedException ex) {
            log.error("Application got interrupted", ex);
        } finally {
            log.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger log = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch, String topic, String bootstrapserver, String groupId){
            this.latch = latch;


            //create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // subscriber consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try{
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Key: " + record.key() + " Value: " + record.value());
                        log.info("Partition: " + record.partition() + " Offset: " + record.offset());
                    }
                }
            } catch (WakeupException ex) {
                log.error("Recieved Shutdown signal");
            } finally {
                consumer.close();
                // tell our main code we're done with consumer
                latch.countDown();
            }
        }

        public void shutdown(){
                //wake uo method is a special method to interrupt consumer.poll()
            //it will thorw the exception WakeUpException
            consumer.wakeup();
        }
    }
}

package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args){

        Producer<String, String> producer = createKafkaProducer();
        for(int i=0;i<1000;i++){
            System.out.println("发消息啦:"+i);
            // send（）方法是异步的。调用时，它会将记录添加到待处理记录发送的缓冲区中并立即返回。这允许生产者将各个记录一起批处理以提高效率。
            producer.send(new ProducerRecord("my-replicated3-topic","{myMsg:hello i am a messge,index:" + i + "}"),new DemoCallBack());
        }
        //生成器包含一个缓冲区空间池，用于保存尚未传输到服务器的记录，以及一个后台I / O线程，负责将这些记录转换为请求并将它们传输到集群。未能在使用后关闭生产者将泄漏这些资源。
        producer.close();


        KafkaConsumer consumer = createKafkaConsumer();
        consumer.subscribe(Arrays.asList("my-replicated3-topic"));
        while (true) {
            System.out.println("拉取消息");
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("收消息啦:offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }



    }

    private static Producer<String,String> createKafkaProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.21.91:9090,192.168.21.92:9090,192.168.21.93:9090");
        //acks配置控制认为请求完成的标准。我们指定的“all”设置将导致阻止完整提交记录，这是最慢但最耐用的设置。
        props.put("acks", "all");
        //如果请求失败，则生产者可以自动重试，但由于我们已将重试指定为0，因此不会。启用重试也会打开重复的可能性（有关详细信息，请参阅有关消息传递语义的文档）。
        props.put("retries", 0);
        // 生产者为每个分区维护未发送记录的缓冲区。这些缓冲区的大小由batch.size配置指定。使这个更大可以导致更多的批处理，但需要更多的内存（因为我们通常会为每个活动分区提供这些缓冲区之一）。
        // 默认情况下，即使缓冲区中有其他未使用的空间，也可以立即发送缓冲区。
        props.put("batch.size", 16384);
        // 但是，如果您想减少请求数量，可以将linger.ms设置为大于0的值。这将指示生产者在发送请求之前等待该毫秒数，希望更多记录到达以填满同一批次。
        // 这类似于TCP中的Nagle算法。例如，在上面的代码片段中，由于我们将逗留时间设置为1毫秒，因此可能会在单个请求中发送所有100条记录。
        // 但是，如果我们没有填满缓冲区，此设置会为我们的请求增加1毫秒的延迟，等待更多记录到达。
        // 请注意，即使在linger.ms = 0的情况下，及时到达的记录通常也会一起批处理，因此在重负载情况下，无论延迟配置如何，都会发生批处理;
        // 但是，将此值设置为大于0的值可以在不受最大负载影响的情况下以较少的延迟为代价导致更少，更有效的请求。
        props.put("linger.ms", 10);
        // buffer.memory控制生产者可用于缓冲的总内存量。如果记录的发送速度快于传输到服务器的速度，则此缓冲区空间将耗尽。当缓冲区空间耗尽时，额外的发送调用将被阻止。
        // 阻塞时间的阈值由max.block.ms确定，然后抛出TimeoutException。
        props.put("buffer.memory", 33554432);
        // key.serializer和value.serializer指示如何将用户提供的键和值对象及其ProducerRecord转换为字节。您可以将包含的ByteArraySerializer或StringSerializer用于简单的字符串或字节类型。
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 从Kafka 0.11开始，KafkaProducer支持另外两种模式：幂等生成器和事务生成器。幂等生产者将卡夫卡的交付语义从至少一次加强到一次交付。
        // 特别是生产者重试将不再引入重复。事务生成器允许应用程序以原子方式将消息发送到多个分区（和主题！）。
        // 要启用幂等性，必须将enable.idempotence配置设置为true。如果设置，则重试配置将默认为Integer.MAX_VALUE，并且acks配置将默认为all。
        // 幂等生成器没有API更改，因此不需要修改现有应用程序以利用此功能。
        // 要利用幂等生成器，必须避免应用程序级别重新发送，因为这些不能重复数据删除。因此，如果应用程序启用幂等性，建议将重试配置保留为未设置，因为它将默认为Integer.MAX_VALUE。
        // 此外，如果send（ProducerRecord）即使无限次重试也会返回错误（例如，如果消息在发送之前在缓冲区中到期），则建议关闭生产者并检查上次生成的消息的内容以确保它没有重复。
        // 最后，生产者只能保证在单个会话中发送的消息的幂等性。
        // 要使用事务生成器和话务员API，必须设置transactional.id配置属性。如果设置了transactional.id，则会自动启用idempotence以及idempotence所依赖的生产者配置。
        // 此外，交易中包含的主题应配置为持久性。特别是，replication.factor应至少为3，并且这些主题的min.insync.replicas应设置为2.
        // 最后，为了从端到端实现事务保证，消费者必须是配置为只读取已提交的消息。

        Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
        return kafkaProducer;
    }

    /**
     * 支持事务的生产者
     *
     * @return
     */
    public static KafkaProducer createTransactionKafkaProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("transactional.id", "my-transactional-id");
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 100; i++)
                producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
    }

    public static KafkaConsumer createKafkaConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.21.91:9090,192.168.21.92:9090,192.168.21.93:9090");
        props.put("group.id", "consume-group1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout", "3000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

}

class DemoCallBack implements Callback{

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("消息发送成功,offset位置:"+recordMetadata.offset());
    }
}
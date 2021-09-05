package nl.delftdata.stateflow_flink.proto;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaSerializeProto implements KafkaSerializationSchema<EventOuterClass.Event> {

    private String topic;

    public KafkaSerializeProto(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        KafkaSerializationSchema.super.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(EventOuterClass.Event event, @Nullable Long aLong) {
        return new ProducerRecord<byte[], byte[]>(this.topic, event.getEventId().getBytes(StandardCharsets.UTF_8), event.toByteString().toByteArray());
    }
}

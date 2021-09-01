package nl.delftdata.stateflow_flink.proto;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class KafkaDeserializeProto implements DeserializationSchema<EventOuterClass.Event> {

    @Override
    public EventOuterClass.Event deserialize(byte[] bytes) throws IOException {
        return EventOuterClass.Event.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(EventOuterClass.Event event) {
        return false;
    }

    @Override
    public TypeInformation<EventOuterClass.Event> getProducedType() {
        return TypeInformation.of(EventOuterClass.Event.class);
    }
}

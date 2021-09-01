package nl.delftdata.stateflow_flink;

import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import nl.delftdata.stateflow_flink.proto.KafkaDeserializeProto;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

public class StateFlowFlinkJob {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Builds the Kafka Source
        KafkaSource<EventOuterClass.Event> source = KafkaSource.<EventOuterClass.Event>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("internal", "client-request")
                .setGroupId("flink-group-id")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new KafkaDeserializeProto())
                .build();

        String[] operators = null;
        List<OutputTag<EventOuterClass.Route>> outputs = new ArrayList<>();

        for (String operator : operators) {
            outputs.add(new OutputTag<EventOuterClass.Route>(operator + "-output"));
            outputs.add(new OutputTag<EventOuterClass.Route>(operator + "-create-output"));
        }

        DataStream<EventOuterClass.Route> routed_events = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka-Source")
                .process(new IngressRouter());

        // Decide which routes we push to which stream
        // Then for each operator build 2 pipelines (one with create, one without).
        // Link each up to a KafkaSource.
        // Build an InvokeStatelessLambda (this can be async).

        // Test and deploy this job on a cluster, see what it looks like :)
        //

    }
}

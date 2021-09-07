package nl.delftdata.stateflow_flink;

import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import nl.delftdata.stateflow_flink.proto.KafkaDeserializeProto;
import nl.delftdata.stateflow_flink.proto.KafkaSerializeProto;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StateFlowFlinkJob {
  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    if (!parameter.has("operators")) {
      throw new RuntimeException("Ensure that operators is set.");
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Configuration conf = new Configuration();
//    conf.setDouble("taskmanager.network.memory.fraction", 0.4);
    conf.setString("execution.buffer-timeout", "0");
    conf.setString("execution.batch-shuffle-mode", "ALL_EXCHANGES_PIPELINED");
//    StreamExecutionEnvironment env =
//        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
    env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
    env.getConfig().setGlobalJobParameters(parameter);
    
    TypeInformation<EventOuterClass.Route> routeTypeInformation =
        TypeInformation.of(EventOuterClass.Route.class);

    // Builds the Kafka Source
    KafkaSource<EventOuterClass.Event> source =
        KafkaSource.<EventOuterClass.Event>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("internal", "client_request")
            .setGroupId("flink-group-id")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new KafkaDeserializeProto())
            .build();

    String[] operators = parameter.get("operators").split(",");
    HashMap<String, OutputTag<EventOuterClass.Route>> outputs = new HashMap<>();

    for (String operator : operators) {
      outputs.put(
          operator,
          new OutputTag<EventOuterClass.Route>(operator + "-output", routeTypeInformation));
      outputs.put(
          operator + "-create",
          new OutputTag<EventOuterClass.Route>(operator + "-create-output", routeTypeInformation));
    }

    SingleOutputStreamOperator<EventOuterClass.Route> routedEvents =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka-Source")
            .process(new IngressRouter(outputs))
            .name("Ingress routing.");

    // Decide which routes we push to which stream
    // Then for each operator build 2 pipelines (one with create, one without).
    // Link each up to a KafkaSource.
    // Build an InvokeStatelessLambda (this can be async).
    List<DataStream<EventOuterClass.Event>> operatorOutputs = new ArrayList<>();

    for (String operator : operators) {
      // First we build the 'create-flow'. We get the 'create' stream.
      DataStream<EventOuterClass.Route> createOperatorsRaw =
          routedEvents.getSideOutput(outputs.get(operator + "-create"));

      // We follow the timeout settings of AWS (6 seconds).
      DataStream<EventOuterClass.Route> createOperatorsProcessed =
          AsyncDataStream.unorderedWait(
                  createOperatorsRaw, new InvokeStatelessLambda(parameter.get("aws-function-name")), 2, TimeUnit.SECONDS, 1000)
              .map(
                  e ->
                      EventOuterClass.Route.newBuilder()
                          .setKey(e.getFunAddress().getKey())
                              .setRouteName(StateFlowFlinkJob.getFullName(e.getFunAddress()))
                          .setEventValue(e)
                          .build())
              .name("Create " + operator);

      // Then we get the operator stream.
      DataStream<EventOuterClass.Route> operatorsRaw =
          routedEvents.getSideOutput(outputs.get(operator));

      DataStream<EventOuterClass.Event> operatorsProcessed =
          operatorsRaw
              .union(createOperatorsProcessed)
              .keyBy(r -> r.getKey())
              .process(new InvokeStatefulLambda(parameter.get("aws-function-name")))
              .name("Invoke stateful " + operator);
      operatorOutputs.add(operatorsProcessed);
    }

    // Here we link the routed_events directly to the egress router.
    OutputTag<EventOuterClass.Route> internal_tag =
        new OutputTag<EventOuterClass.Route>("internal-to-kafka", routeTypeInformation);
    EgressRouter egressRouter = new EgressRouter(internal_tag);

    DataStream<EventOuterClass.Event>[] operatorOutputsArray =
        new DataStream[operatorOutputs.size()];
    SingleOutputStreamOperator<EventOuterClass.Route> egressEvents =
        routedEvents
            .map(r -> r.getEventValue())
            .union(operatorOutputs.toArray(operatorOutputsArray))
            .process(egressRouter)
            .name("Egress routing.");

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");

    FlinkKafkaProducer<EventOuterClass.Event> internal_kafka_sink =
        new FlinkKafkaProducer<>(
            "internal", // target topic
            new KafkaSerializeProto("internal"), // serialization schema
            properties, // producer config
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE); // fault-tolerance

    FlinkKafkaProducer<EventOuterClass.Event> client_kafka_sink =
        new FlinkKafkaProducer<>(
            "client_reply", // target topic
            new KafkaSerializeProto("client_reply"), // serialization schema
            properties, // producer config
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE); // fault-tolerance

    // Add sink for the client and the internal topic.
    egressEvents.map(r -> r.getEventValue()).addSink(client_kafka_sink).name("Client Kafka Sink.");
    egressEvents
        .getSideOutput(internal_tag)
        .map(r -> r.getEventValue())
        .addSink(internal_kafka_sink)
        .name("Internal Kafka Sink.");

    env.execute();
  }

  public static String getFullName(EventOuterClass.FunctionAddress functionAddress) {
    return functionAddress.getFunType().getNamespace()
            + "/"
            + functionAddress.getFunType().getName();
  }
}

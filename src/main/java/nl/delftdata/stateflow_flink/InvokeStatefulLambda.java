package nl.delftdata.stateflow_flink;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.codahale.metrics.UniformReservoir;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

public class InvokeStatefulLambda extends KeyedProcessFunction<String, EventOuterClass.Route, EventOuterClass.Event> {

    protected static final Logger LOG = LoggerFactory.getLogger(Router.class);

    private String functionName;
    private AWSLambda awsLambda;

    private transient Histogram lambdaLatency;
    private transient ValueState<ByteString> operatorState;
    private transient ObjectMapper mapper;

    public InvokeStatefulLambda(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.awsLambda = AWSLambdaClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(Regions.EU_WEST_2).build();
;
        com.codahale.metrics.Histogram dropwizardHistogram =
                new com.codahale.metrics.Histogram(new UniformReservoir());

        this.lambdaLatency = getRuntimeContext().getMetricGroup().histogram("aws-latency", new DropwizardHistogramWrapper(dropwizardHistogram));

        ValueStateDescriptor<ByteString> descriptor =
                new ValueStateDescriptor<>(
                        "operatorState", // the state name
                        TypeInformation.of(ByteString.class));
        this.operatorState = getRuntimeContext().getState(descriptor);
        this.mapper = new ObjectMapper();
    }

    @Override
    public void processElement(EventOuterClass.Route route, Context context, Collector<EventOuterClass.Event> collector) throws Exception {
        EventOuterClass.Event event = route.getEventValue();
        String operatorName = route.getRouteName();

        LOG.debug("Now invoking stateful " + operatorName + "/" + context.getCurrentKey() + " Current time: " + System.currentTimeMillis());
        if (operatorState.value() == null) {
            operatorState.update(ByteString.EMPTY);
        }

        EventOuterClass.EventRequestReply request =
                EventOuterClass.EventRequestReply.newBuilder()
                        .setEvent(event)
                        .setOperatorName(operatorName)
                        .setState(operatorState.value())
                        .build();

        InvokeRequest invokeRequest =
                new InvokeRequest()
                        .withFunctionName(this.functionName)
                        .withPayload("{\"request\": \"" + Base64.getEncoder().encodeToString(request.toByteArray()) + "\"}");

        long startTime = System.currentTimeMillis();
        InvokeResult invokeResult = awsLambda.invoke(invokeRequest);
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);


        if (invokeResult.getFunctionError() != null) {
            LOG.error(
                    "Error while invoke stateful Lambda: "
                            + invokeResult.getFunctionError()
                            + "\n"
                            + new String(invokeResult.getPayload().array(), "UTF-8"));
        }
        JsonNode payloadJson =  this.mapper.readTree(invokeResult.getPayload().array());

        EventOuterClass.EventRequestReply reply =
                EventOuterClass.EventRequestReply.parseFrom(Base64.getDecoder().decode(payloadJson.get("reply").asText()));

        operatorState.update(reply.getState());

        LOG.debug("That took " + duration + "ms. Current time: " + System.currentTimeMillis());
        this.lambdaLatency.update(duration);

        collector.collect(reply.getEvent());
    }
}

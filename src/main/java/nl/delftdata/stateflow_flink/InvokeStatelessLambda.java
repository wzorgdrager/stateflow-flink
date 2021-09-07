package nl.delftdata.stateflow_flink;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.codahale.metrics.UniformReservoir;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

public class InvokeStatelessLambda
    extends RichAsyncFunction<EventOuterClass.Route, EventOuterClass.Event> {

  public class Reply {
    public Reply(){}

    private String reply;
  }

  protected static final Logger LOG = LoggerFactory.getLogger(Router.class);

  private String functionName;
  private AWSLambda awsLambda;
  private transient Histogram lambdaLatency;
  private transient ObjectMapper mapper;

  public InvokeStatelessLambda(String functionName) {
    this.functionName = functionName;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    System.out.println("Function name is: " + this.functionName);
    this.awsLambda =
        AWSLambdaClientBuilder.standard()
            .withCredentials(new ProfileCredentialsProvider())
            .withRegion(Regions.EU_WEST_2)
            .build();
    ;
    com.codahale.metrics.Histogram dropwizardHistogram =
        new com.codahale.metrics.Histogram(new UniformReservoir());

    this.lambdaLatency =
        getRuntimeContext()
            .getMetricGroup()
            .histogram("aws-latency", new DropwizardHistogramWrapper(dropwizardHistogram));
    this.mapper = new ObjectMapper();
  }

  @Override
  public void asyncInvoke(
      EventOuterClass.Route route, ResultFuture<EventOuterClass.Event> resultFuture)
      throws Exception {
    EventOuterClass.Event event = route.getEventValue();
    String operatorName = getFullName(event.getFunAddress());

    EventOuterClass.EventRequestReply request =
        EventOuterClass.EventRequestReply.newBuilder()
            .setEvent(event)
            .setOperatorName(operatorName)
            .clearState()
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
              "Error while invoke stateless Lambda: "
                      + invokeResult.getFunctionError()
                      + "\n"
                      + new String(invokeResult.getPayload().array(), "UTF-8"));
    }
    JsonNode payloadJson =  this.mapper.readTree(invokeResult.getPayload().array());

    EventOuterClass.EventRequestReply reply =
        EventOuterClass.EventRequestReply.parseFrom(Base64.getDecoder().decode(payloadJson.get("reply").asText()));

    this.lambdaLatency.update(duration);

    resultFuture.complete(Collections.singleton(reply.getEvent()));
  }

  public String getFullName(EventOuterClass.FunctionAddress functionAddress) {
    return functionAddress.getFunType().getNamespace()
        + "/"
        + functionAddress.getFunType().getName();
  }
}

package nl.delftdata.stateflow_flink;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.ServiceException;

public class InvokeLambda extends KeyedProcessFunction<String, EventOuterClass.Route, EventOuterClass.Event> {

    private String functionName;
    private AWSLambda awsLambda;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.functionName = ConfigOptions.key("stateflow.aws-function-name").toString();
        this.awsLambda = AWSLambdaClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(Regions.EU_WEST_2).build();
    }

    @Override
    public void processElement(EventOuterClass.Route route, Context context, Collector<EventOuterClass.Event> collector) throws Exception {
        EventOuterClass.Event event = route.getEventValue();






    }
}

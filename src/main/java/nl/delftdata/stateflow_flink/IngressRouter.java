package nl.delftdata.stateflow_flink;

import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IngressRouter extends ProcessFunction<EventOuterClass.Event, EventOuterClass.Route> {

    private static final Logger LOG = LoggerFactory.getLogger(IngressRouter.class);
    private final HashMap<String, OutputTag<EventOuterClass.Route>> outputs;

    public IngressRouter(HashMap<String, OutputTag<EventOuterClass.Route>> outputs) {
        this.outputs = outputs;
    }

    @Override
    public void processElement(EventOuterClass.Event event, Context context,
                               Collector<EventOuterClass.Route> collector) throws Exception {

        if (event.getEventTypeCase() != EventOuterClass.Event.EventTypeCase.REQUEST) {
            LOG.warn("Trying to route an event which is not a request. The event id " + event.getEventId());
            collector.close();
            return;
        }

        // The 'egress' is the main output
        // Each other output is a sideoutput.

        if (event.getRequest() == EventOuterClass.Request.Ping) {
                collector.collect(EventOuterClass.Route
                        .newBuilder()
                        .setDirection(EventOuterClass.RouteDirection.EGRESS)
                        .setKey(event.getEventId())
                        .setRouteName("")
                        .setEventValue(event.toBuilder().clearRequest().setReplyValue(EventOuterClass.Reply.Pong.getNumber()).build())
                        .build());


        } else if (event.getRequest() == EventOuterClass.Request.EventFlow) {
            EventOuterClass.FunctionAddress currentFun = event.getCurrent().getCurrentFun();
            String routeName = getFullName(currentFun);

            LOG.debug("Current EventFlowNodeType is " + event.getCurrent().getCurrentNodeType());
            if (event.getCurrent().getCurrentNodeType().equals("RETURN")) {
                // Sending back to the client.
                collector.collect(EventOuterClass.Route
                        .newBuilder()
                        .setDirection(EventOuterClass.RouteDirection.EGRESS)
                        .setKey(event.getEventId())
                        .setRouteName(routeName)
                        .setEventValue(event.toBuilder().clearRequest().setReplyValue(EventOuterClass.Reply.SuccessfulInvocation.getNumber()).build())
                        .build());
            } else {
                // Send tot the next operator.
                context.output(this.outputs.get(routeName),
                        EventOuterClass.Route
                        .newBuilder()
                        .setDirection(EventOuterClass.RouteDirection.INTERNAL)
                        .setKey(currentFun.getKey())
                        .setRouteName(routeName)
                        .setEventValue(event)
                        .build());
            }
        } else if (!event.getFunAddress().getKey().equals("")) {
            context.output(this.outputs.get(getFullName(event.getFunAddress())),
                    EventOuterClass.Route
                    .newBuilder()
                    .setDirection(EventOuterClass.RouteDirection.INTERNAL)
                    .setKey(event.getFunAddress().getKey())
                    .setRouteName(getFullName(event.getFunAddress()))
                    .setEventValue(event)
                    .build());
        } else {
            // To deal with create operations.
            context.output(this.outputs.get(getFullName(event.getFunAddress()) + "-create"),
                    EventOuterClass.Route
                    .newBuilder()
                    .setDirection(EventOuterClass.RouteDirection.INTERNAL)
                    .setKey("")
                    .setRouteName(getFullName(event.getFunAddress()))
                    .setEventValue(event)
                    .build());
        }


        collector.close();

    }

    public String getFullName(EventOuterClass.FunctionAddress functionAddress) {
        return functionAddress.getFunType().getNamespace() + "/" + functionAddress.getFunType().getName();
    }
}

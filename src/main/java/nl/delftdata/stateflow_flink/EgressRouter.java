package nl.delftdata.stateflow_flink;

import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class EgressRouter extends Router {

  private final OutputTag<EventOuterClass.Route> internal_tag;

  public EgressRouter(OutputTag<EventOuterClass.Route> internal_tag) {
    this.internal_tag = internal_tag;
  }

  @Override
  public void processElement(
      EventOuterClass.Event event, Context context, Collector<EventOuterClass.Route> collector)
      throws Exception {
    // We have two outputs
    // 1. to the client (via the collector)
    // 2. to the internal topic (via sideoutput).
    System.out.println("Got event " + event);

    EventOuterClass.Route route = this.route(event);

    if (route.getDirection() == EventOuterClass.RouteDirection.CLIENT) {
      collector.collect(this.route(event));
    } else if (route.getDirection() == EventOuterClass.RouteDirection.INTERNAL) {
      context.output(this.internal_tag, route);
    }
  }

  public EventOuterClass.Route route(EventOuterClass.Event event) {
    if (event.getEventTypeCase() == EventOuterClass.Event.EventTypeCase.REQUEST
        && event.getRequest() == EventOuterClass.Request.EventFlow) {
      return routeFlow(event);
    } else if (event.getEventTypeCase() == EventOuterClass.Event.EventTypeCase.REPLY) {
      return EventOuterClass.Route.newBuilder()
          .setDirection(EventOuterClass.RouteDirection.CLIENT)
          .setRouteName("")
          .setKey(event.getEventId())
          .setEventValue(event)
          .build();
    } else {
      throw new RuntimeException(
          "Expected either an EventFlow Request event or Reply, but got " + event.toString());
    }
  }

  public EventOuterClass.Route routeFlow(EventOuterClass.Event event) {
    EventOuterClass.FunctionAddress currentFun = event.getCurrent().getCurrentFun();
    String routeName = getFullName(currentFun);

    this.LOG.debug("Current EventFlowNodeType is " + event.getCurrent().getCurrentNodeType());

    // TODO: Make sure line 58 to 92 is done in the AWS Lambda instance.

    return EventOuterClass.Route.newBuilder()
        .setRouteName("")
        .setDirection(EventOuterClass.RouteDirection.INTERNAL)
        .setKey(event.getEventId())
        .setEventValue(event)
        .build();
  }
}

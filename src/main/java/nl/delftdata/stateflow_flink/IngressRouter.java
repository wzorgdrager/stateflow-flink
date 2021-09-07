package nl.delftdata.stateflow_flink;

import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

public class IngressRouter extends Router {

  private final HashMap<String, OutputTag<EventOuterClass.Route>> outputs;

  public IngressRouter(HashMap<String, OutputTag<EventOuterClass.Route>> outputs) {
    this.outputs = outputs;
  }

  @Override
  public void processElement(
      EventOuterClass.Event event, Context context, Collector<EventOuterClass.Route> collector)
      throws Exception {

    LOG.debug("Now reading event " + event + " " + System.currentTimeMillis());
    EventOuterClass.Route route = this.route(event);

    if (route.getDirection() == EventOuterClass.RouteDirection.EGRESS) {
      collector.collect(route);
    } else if (route.getDirection() == EventOuterClass.RouteDirection.INTERNAL) {
      LOG.debug("Sending event to " + route.getRouteName() + " with key " + route.getKey() +".");
      context.output(this.outputs.get(route.getRouteName()), route);
    } else {
      throw new RuntimeException("Unknown route. The event id " + event.getEventId());
    }
  }

  @Override
  public EventOuterClass.Route route(EventOuterClass.Event event) {
    if (event.getEventTypeCase() != EventOuterClass.Event.EventTypeCase.REQUEST) {
      throw new RuntimeException(
          "Trying to route an event which is not a request. The event id " + event.getEventId());
    }

    if (event.getRequest() == EventOuterClass.Request.Ping) {
      return EventOuterClass.Route.newBuilder()
          .setDirection(EventOuterClass.RouteDirection.EGRESS)
          .setKey(event.getEventId())
          .setRouteName("")
          .setEventValue(
              event.toBuilder()
                  .clearRequest()
                  .setReplyValue(EventOuterClass.Reply.Pong.getNumber())
                  .build())
          .build();
    } else if (event.getRequest() == EventOuterClass.Request.EventFlow) {
      return routeFlow(event);
    } else if (!event.getFunAddress().getKey().equals("")) {
      return EventOuterClass.Route.newBuilder()
          .setDirection(EventOuterClass.RouteDirection.INTERNAL)
          .setKey(event.getFunAddress().getKey())
          .setRouteName(getFullName(event.getFunAddress()))
          .setEventValue(event)
          .build();
    } else {
      // To deal with create operations.
      return EventOuterClass.Route.newBuilder()
          .setDirection(EventOuterClass.RouteDirection.INTERNAL)
          .setKey("")
          .setRouteName(getFullName(event.getFunAddress()) + "-create")
          .setEventValue(event)
          .build();
    }
  }

  @Override
  public EventOuterClass.Route routeFlow(EventOuterClass.Event event) {
    EventOuterClass.FunctionAddress currentFun = event.getCurrent().getCurrentFun();
    String routeName = getFullName(currentFun);

    LOG.debug("Current EventFlowNodeType is " + event.getCurrent().getCurrentNodeType());
    if (event.getCurrent().getCurrentNodeType().equals("RETURN")) {
      // Sending back to the client.
      return EventOuterClass.Route.newBuilder()
          .setDirection(EventOuterClass.RouteDirection.EGRESS)
          .setKey(event.getEventId())
          .setRouteName(routeName)
          .setEventValue(
              event.toBuilder()
                  .clearRequest()
                  .setReplyValue(EventOuterClass.Reply.SuccessfulInvocation.getNumber())
                  .build())
          .build();
    } else {
      // Send tot the next operator.
      return EventOuterClass.Route.newBuilder()
          .setDirection(EventOuterClass.RouteDirection.INTERNAL)
          .setKey(currentFun.getKey())
          .setRouteName(routeName)
          .setEventValue(event)
          .build();
    }
  }
}

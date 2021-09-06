package nl.delftdata.stateflow_flink;

import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Router extends ProcessFunction<EventOuterClass.Event, EventOuterClass.Route> {

  protected static final Logger LOG = LoggerFactory.getLogger(Router.class);

  public abstract EventOuterClass.Route route(EventOuterClass.Event event);

  public abstract EventOuterClass.Route routeFlow(EventOuterClass.Event event);

  public String getFullName(EventOuterClass.FunctionAddress functionAddress) {
    return functionAddress.getFunType().getNamespace()
        + "/"
        + functionAddress.getFunType().getName();
  }
}

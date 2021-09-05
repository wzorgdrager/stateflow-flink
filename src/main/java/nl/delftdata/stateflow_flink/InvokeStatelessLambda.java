package nl.delftdata.stateflow_flink;

import nl.delftdata.stateflow_flink.proto.EventOuterClass;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class InvokeStatelessLambda extends RichAsyncFunction<EventOuterClass.Route, EventOuterClass.Event> {

    @Override
    public void asyncInvoke(EventOuterClass.Route route, ResultFuture<EventOuterClass.Event> resultFuture) throws Exception {

    }
}

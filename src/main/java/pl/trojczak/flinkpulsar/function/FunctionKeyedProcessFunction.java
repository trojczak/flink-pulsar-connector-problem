package pl.trojczak.flinkpulsar.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.trojczak.flinkpulsar.model.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class FunctionKeyedProcessFunction extends KeyedProcessFunction<String, Event, Event> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionKeyedProcessFunction.class);
    private static final Random RANDOM = new Random(12345);

    private ListState<Event> storedEvents;

    @Override
    public void open(Configuration parameters) throws Exception {
        storedEvents = getRuntimeContext().getListState(new ListStateDescriptor<>("stored-events", Event.class));
    }

    @Override
    public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
        LOGGER.info("New event in function: Event(id={}, key={}, action={})",
                value.getId(), value.getKey(), value.getAction());

        switch (value.getAction().toString()) {
            case "THROW_EXCEPTION":
                if (RANDOM.nextInt(3) == 0) {
                    throw new IllegalArgumentException("THROW_EXCEPTION 1/3 times...");
                }
                break;
            case "STORE":
                storedEvents.add(value);
                break;
            case "READ":
                List<Event> storedEvents = getListFromIterable(this.storedEvents.get());
                List<String> serializedEvents = storedEvents.stream()
                        .map(event -> String.format("Event(id=%s, action=%s)", event.getId(), event.getAction()))
                        .collect(Collectors.toList());

                LOGGER.info("Stored events ({}): {}", storedEvents.size(), serializedEvents);
                break;
        }
        out.collect(value);
    }

    private List<Event> getListFromIterable(Iterable<Event> events) {
        List<Event> list = new ArrayList<>();
        events.forEach(list::add);
        return list;
    }
}

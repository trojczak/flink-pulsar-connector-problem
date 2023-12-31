package pl.trojczak.flinkpulsar;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.PulsarSinkBuilder;
import org.apache.flink.connector.pulsar.sink.PulsarSinkOptions;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.PulsarSourceBuilder;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.trojczak.flinkpulsar.function.FunctionKeyedProcessFunction;
import pl.trojczak.flinkpulsar.model.Event;

public class RtkTest001Job extends BaseJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(RtkTest001Job.class);

    private static final String ADMIN_URL = "http://localhost:8080";
    private static final String ADMIN_URL_ENV = "PULSAR_ADMIN_URL";
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String SERVICE_URL_ENV = "PULSAR_SERVICE_URL";
    private static final String INPUT_TOPIC = "persistent://rtk/test001/input";
    private static final String OUTPUT_TOPIC = "persistent://rtk/test001/output";
    private static final String SUBSCRIPTION = "input-sub";

    public static void main(String[] args) throws Exception {
        PulsarSource<Event> eventPulsarSource = createPulsarSource(INPUT_TOPIC, SUBSCRIPTION, Event.class);
        Sink<Event> eventPulsarSink = createPulsarSink(OUTPUT_TOPIC, Event.class);

        StreamExecutionEnvironment environment = prepareEnvironment();
        new RtkTest001Job(eventPulsarSource, eventPulsarSink).build(environment);
        environment.execute();
    }

    private final PulsarSource<Event> source;
    private final Sink<Event> sink;

    public RtkTest001Job(PulsarSource<Event> source, Sink<Event> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void build(StreamExecutionEnvironment environment) {
        String sourceUid = "sourceUid";
        DataStream<Event> eventDataStream =
                environment.fromSource(source, WatermarkStrategy.noWatermarks(), sourceUid)
                        .uid(sourceUid);

        String functionUid = "functionUid";
        SingleOutputStreamOperator<Event> processedEvents = eventDataStream
                .keyBy(event -> event.getKey().toString())
                .process(new FunctionKeyedProcessFunction())
                .uid(functionUid);

        String sinkUid = "sinkUid";
        processedEvents.sinkTo(sink)
                .name(sinkUid)
                .uid(sinkUid);
    }

    protected static <IN> PulsarSource<IN> createPulsarSource(String topic, String subscription, Class<IN> inClass) {
        PulsarSourceBuilder<IN> builder = PulsarSource.builder()
                .setConsumerName("CONSUMER" + topic + "_" + inClass.getSimpleName())
                .setAdminUrl(getAdminUrl())
                .setServiceUrl(getServiceUrl())
                .setTopics(topic)
                .setDeserializationSchema(PulsarDeserializationSchema.pulsarSchema(Schema.AVRO(inClass), inClass))
//                .setDeserializationSchema(Schema.AVRO(inClass), inClass)
                .setSubscriptionName(subscription)
                .setSubscriptionType(SubscriptionType.Shared)
                .setConfig(PulsarSourceOptions.PULSAR_ACK_RECEIPT_ENABLED, true)
                .setConfig(PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS, 1)
                .setConfig(PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE, false)
        ;

        Configuration authenticationConfiguration = prepareAuthentication();
        if (authenticationConfiguration != null) {
            builder.setConfig(authenticationConfiguration);
        }

        return builder.build();
    }

    private static <IN> Sink<IN> createPulsarSink(String topic, Class<IN> outClass) {
        PulsarSinkBuilder<IN> builder = PulsarSink.builder()
                .setProducerName("PRODUCER" + topic + "_" + outClass.getSimpleName())
                .enableSchemaEvolution()
                .setAdminUrl(getAdminUrl())
                .setServiceUrl(getServiceUrl())
                .setTopics(topic)
                .setSerializationSchema(PulsarSerializationSchema.pulsarSchema(Schema.AVRO(outClass), outClass))
//                .setSerializationSchema(Schema.AVRO(outClass), outClass)
                .setConfig(PulsarSinkOptions.PULSAR_WRITE_DELIVERY_GUARANTEE, DeliveryGuarantee.EXACTLY_ONCE)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE);

        Configuration authenticationConfiguration = prepareAuthentication();
        if (authenticationConfiguration != null) {
            builder.setConfig(authenticationConfiguration);
        }

        return builder.build();
    }

    private static Configuration prepareAuthentication() {
        Configuration configuration = new Configuration();
        // TODO
        return configuration;
    }

    private static String getAdminUrl() {
        String adminUrl = System.getenv(ADMIN_URL_ENV);
        if (adminUrl == null) {
            adminUrl = ADMIN_URL;
        }
        LOGGER.info(">>> ADMIN_URL: {}", adminUrl);
        return adminUrl;
    }

    private static String getServiceUrl() {
        String serviceUrl = System.getenv(SERVICE_URL_ENV);
        if (serviceUrl == null) {
            serviceUrl = SERVICE_URL;
        }
        LOGGER.info(">>> SERVICE_URL: {}", serviceUrl);
        return serviceUrl;
    }
}

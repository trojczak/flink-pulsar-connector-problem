package pl.trojczak.flinkpulsar.producer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import pl.trojczak.flinkpulsar.model.Event;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static pl.trojczak.flinkpulsar.Commons.INPUT_TOPIC;

public class TmeEnvEventProducer {

    private static final Random RANDOM = new Random(12345);
    private static final String DO_NOTHING = "DO_NOTHING";
    private static final String READ = "READ";
    private static final String STORE = "STORE";
    private static final String THROW_EXCEPTION = "THROW_EXCEPTION";
    private static final String PULSAR_BROKER_URL = "pulsar+ssl://dev-05cb4436-c5c4-4010-848b-74e7a6bc2c73.aws-euw1-snci-duck-prod-snc.aws.snio.cloud:6651";
    private static final String AUTHENTICATION_OAUTH2 = "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2";

    public static void main(String[] args) throws PulsarClientException {
        TmeEnvEventProducer eventProducer = new TmeEnvEventProducer();

        List<Event> events = eventProducer.prepareEvents();
        for (Event event : events) {
            System.out.printf("Producing event: Event(id=%d, key=%s, action=%s)%n",
                    event.getId(), event.getKey(), event.getAction());
            eventProducer.produce(event);
        }

        eventProducer.close();
    }

    private final PulsarClient client;
    private final Producer<Event> producer;

    public TmeEnvEventProducer() throws PulsarClientException {
        this.client = PulsarClient.builder().serviceUrl(PULSAR_BROKER_URL)
            .authentication(AUTHENTICATION_OAUTH2, prepareAuthenticationData())
            .build();
        this.producer = client.newProducer(Schema.AVRO(Event.class))
            .topic(INPUT_TOPIC)
            .create();
    }

    public List<Event> prepareEvents() {
        String content = getContent();

        List<Event> events = new ArrayList<>();

        for (long i = 1; i <= 100; i++) {
            events.add(new Event(i, "ID1", content, getRandomAction()));
        }

        return events;
    }

    private static String getContent() {
        String content;
        try {
            content = Files.readString(Path.of("data/lorem.txt"));
        } catch (IOException ex) {
            throw new RuntimeException("Unable to read the lorem file.", ex);
        }
        return content;
    }

    public void produce(Event event) throws PulsarClientException {
        this.producer.newMessage().value(event).send();
    }

    public void close() throws PulsarClientException {
        this.producer.close();
        this.client.close();
    }

    private String getRandomAction() {
        List<String> actions = List.of(DO_NOTHING, DO_NOTHING, DO_NOTHING, DO_NOTHING, DO_NOTHING, READ, READ, READ, STORE, THROW_EXCEPTION);
        int randomIndex = RANDOM.nextInt(actions.size());
        return actions.get(randomIndex);
    }

    private static String prepareAuthenticationData() {
        return "{\"issuerUrl\":\"https://auth.streamnative.cloud/\",\"audience\":\"urn:sn:pulsar:tme:hosted-dev\",\"privateKey\":\"file:///home/rtk/tme-admin.json\"}";
    }
}

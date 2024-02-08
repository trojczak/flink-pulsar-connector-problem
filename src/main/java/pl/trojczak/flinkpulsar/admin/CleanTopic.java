package pl.trojczak.flinkpulsar.admin;

import org.apache.pulsar.client.api.PulsarClientException;
import pl.trojczak.pulsar.common.admin.PulsarAdminClient;

import static pl.trojczak.flinkpulsar.Commons.*;
import static pl.trojczak.flinkpulsar.common.Commons.NAMESPACE;
import static pl.trojczak.flinkpulsar.common.Commons.TENANT;

public class CleanTopic {

    private static final String SCHEMA_PATH = "src/main/resources/schemas/avro/";
    private static final String RAW_VC_EVENT_TOPIC_SCHEMA = SCHEMA_PATH + "Event.avsc";

    public static void main(String[] args) throws PulsarClientException {
        try (PulsarAdminClient pulsarAdminClient = new PulsarAdminClient()) {
            pulsarAdminClient.createTenant(TENANT);
            pulsarAdminClient.createNamespace(TENANT, NAMESPACE);

            pulsarAdminClient.deleteTopic(TENANT + "/" + NAMESPACE, INPUT_TOPIC);
            pulsarAdminClient.deleteTopic(TENANT + "/" + NAMESPACE, OUTPUT_TOPIC);

            pulsarAdminClient.createPartitionedTopic(TENANT, NAMESPACE, INPUT_TOPIC, 1);
            pulsarAdminClient.createSubscriptionFromEarliest(INPUT_TOPIC, INPUT_SUB_NAME);
            pulsarAdminClient.createSchema(INPUT_TOPIC, RAW_VC_EVENT_TOPIC_SCHEMA);

            pulsarAdminClient.createPartitionedTopic(TENANT, NAMESPACE, OUTPUT_TOPIC, 1);
            pulsarAdminClient.createSchema(OUTPUT_TOPIC, RAW_VC_EVENT_TOPIC_SCHEMA);
        }
    }
}

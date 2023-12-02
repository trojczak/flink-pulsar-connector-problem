package pl.trojczak.flinkpulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static pl.trojczak.flinkpulsar.Commons.*;

public class CleanTopic {

    private static final Logger LOGGER = LoggerFactory.getLogger(CleanTopic.class);

    public static void main(String[] args) throws PulsarClientException {
        try (PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(PULSAR_LOCAL_SERVICE_HTTP_URL).build()) {
            createTopic(pulsarAdmin, INPUT_TOPIC, INPUT_SUB_NAME);
            createTopic(pulsarAdmin, OUTPUT_TOPIC, null);
        }
    }

    private static void createTopic(PulsarAdmin pulsarAdmin, String topic, String subName) {
        try {
            pulsarAdmin.topics().deletePartitionedTopic(topic, true);
        } catch (Exception ex) {
            // Ignore
        }
        try {
            pulsarAdmin.topics().createPartitionedTopic(topic, 1);
            if (subName != null) {
                pulsarAdmin.topics().createSubscription(topic, subName, MessageId.earliest);
            }
        } catch (PulsarAdminException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}

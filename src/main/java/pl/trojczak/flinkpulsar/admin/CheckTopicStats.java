package pl.trojczak.flinkpulsar.admin;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import pl.trojczak.pulsar.common.admin.PulsarAdminClient;
import pl.trojczak.pulsar.common.model.SubscriptionStats;
import pl.trojczak.pulsar.common.model.TopicStats;

import java.util.Map;

import static pl.trojczak.flinkpulsar.Commons.INPUT_TOPIC;
import static pl.trojczak.flinkpulsar.Commons.OUTPUT_TOPIC;

public class CheckTopicStats {

    public static void main(String[] args) throws PulsarClientException {
        try (PulsarAdminClient pulsarAdminClient = new PulsarAdminClient()) {
            TopicStats inputTopicStats = pulsarAdminClient.getStatsForPartitionedTopic(INPUT_TOPIC);
            printTopicStats(INPUT_TOPIC, inputTopicStats);

            TopicStats outputTopicStats = pulsarAdminClient.getStatsForPartitionedTopic(OUTPUT_TOPIC);
            printTopicStats(OUTPUT_TOPIC, outputTopicStats);
        }
    }

    private static void printTopicStats(String topic, TopicStats topicStats) {
        System.out.println("=== STATS for " + topic + " ===");
        System.out.println("Backlog size:    " + topicStats.getBacklogSize());
        System.out.println("Msg in counter:  " + topicStats.getMsgInCounter());
        System.out.println("Msg out counter: " + topicStats.getMsgOutCounter());
        System.out.println("== Subscriptions ==");
        for (Map.Entry<String, SubscriptionStats> subscription : topicStats.getSubscriptionStats().entrySet()) {
            String subscriptionKey = subscription.getKey();
            SubscriptionStats subscriptionStats = subscription.getValue();
            System.out.println("= " + subscriptionKey + " Sub =");
            System.out.println(" Backlog size: " + subscriptionStats.getBacklogSize());
        }
        System.out.println();
    }
}

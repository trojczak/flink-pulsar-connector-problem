package pl.trojczak.flinkpulsar.admin;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.*;

import java.util.Map;

import static pl.trojczak.flinkpulsar.Commons.*;

public class CheckTopicStats {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {
        try (PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(PULSAR_LOCAL_SERVICE_HTTP_URL).build()) {
            printTopicStats(pulsarAdmin, INPUT_TOPIC);
            printTopicStats(pulsarAdmin, OUTPUT_TOPIC);
        }
    }

    private static void printTopicStats(PulsarAdmin pulsarAdmin, String topic) throws PulsarAdminException {
        TopicStats stats = pulsarAdmin.topics().getPartitionedStats(topic, false, true, true, true);
        PartitionedTopicInternalStats internalStats = pulsarAdmin.topics().getPartitionedInternalStats(topic);
        System.out.println("=== STATS for " + topic + " ===");
        System.out.println("Backlog size:    " + stats.getBacklogSize());
        System.out.println("Msg in counter:  " + stats.getMsgInCounter());
        System.out.println("Msg out counter: " + stats.getMsgOutCounter());
        System.out.println("== Subscriptions ==");
        for (Map.Entry<String, ? extends SubscriptionStats> subscription : stats.getSubscriptions().entrySet()) {
            String subscriptionKey = subscription.getKey();
            SubscriptionStats subscriptionStats = subscription.getValue();
            System.out.println("= " + subscriptionKey + " Sub =");
            System.out.println(" Backlog size: " + subscriptionStats.getBacklogSize());
        }
        for (Map.Entry<String, PersistentTopicInternalStats> entry : internalStats.partitions.entrySet()) {
            PersistentTopicInternalStats persistentTopicInternalStats = entry.getValue();
            for (Map.Entry<String, ManagedLedgerInternalStats.CursorStats> entryCursorStats :
                    persistentTopicInternalStats.cursors.entrySet()) {
                String cursorStatsKey = entryCursorStats.getKey();
                System.out.println("= " + cursorStatsKey + " =");
                ManagedLedgerInternalStats.CursorStats cursorStats = entryCursorStats.getValue();
                System.out.println(" Read position: " + cursorStats.readPosition);
            }
        }
        System.out.println();
    }
}

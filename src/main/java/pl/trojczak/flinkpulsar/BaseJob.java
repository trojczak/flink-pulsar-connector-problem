package pl.trojczak.flinkpulsar;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BaseJob {

    private static final int CHECKPOINTING_INTERVAL = 5_000;
    private static final String DEFAULT_MEMORY = "256MB";
    private static final String ENV = "ENV";
    private static final String LOCAL_ENV = "local";

    public static StreamExecutionEnvironment prepareEnvironment() {
        String env = System.getenv(ENV);

        StreamExecutionEnvironment environment;
        if (LOCAL_ENV.equals(env)) {
            Configuration configuration = new Configuration();
            configuration.set(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.2f);
            configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse(DEFAULT_MEMORY));
            configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse(DEFAULT_MEMORY));
            environment = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        } else {
            environment = StreamExecutionEnvironment.getExecutionEnvironment();
        }
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.enableCheckpointing(CHECKPOINTING_INTERVAL, CheckpointingMode.EXACTLY_ONCE);
        return environment;
    }
}

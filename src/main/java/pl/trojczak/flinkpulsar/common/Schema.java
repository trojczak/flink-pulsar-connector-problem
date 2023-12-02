package pl.trojczak.flinkpulsar.common;

import lombok.Data;

import java.util.Map;

@Data
public class Schema {

    private String type = "AVRO";
    private String schema;
    private Map<String, String> properties = Map.of(
            "__alwaysAllowNull", "true",
            "__jsr310ConversionEnabled", "false"
    );
}

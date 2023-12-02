package pl.trojczak.flinkpulsar.app;

import pl.trojczak.flinkpulsar.common.PulsarAvroSchemaCreator;

import java.io.IOException;

public class PrepareAvroSchemaMain {

    public static void main(String[] args) throws IOException {
        PulsarAvroSchemaCreator.preparePulsarSchemaFromAvsc("src/main/resources/schemas/avro/Event.avsc");
    }
}

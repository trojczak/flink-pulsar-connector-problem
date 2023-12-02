package pl.trojczak.flinkpulsar.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
public class PulsarAvroSchemaCreator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String OUTPUT_PATH_DIR_NAME = "output";
    private static final Path OUTPUT_PATH = prepareOutputPath();

    public static void preparePulsarSchemaFromAvsc(String avscPath) throws IOException {
        Path schemaPath = Path.of(avscPath);
        String schemaContent = Files.readString(schemaPath);
        Schema schema = new Schema();
        schema.setSchema(schemaContent.replaceAll("\\s", ""));

        String fileName = schemaPath.getFileName().toString();
        String outputSchemaFileName = fileName.substring(0, fileName.lastIndexOf(".")) + ".json";
        Path outputFilePath = OUTPUT_PATH.resolve(outputSchemaFileName);

        OBJECT_MAPPER.writeValue(outputFilePath.toFile(), schema);
    }

    private static Path prepareOutputPath() {
        Path outputPath = Path.of(OUTPUT_PATH_DIR_NAME);
        if (!Files.exists(outputPath)) {
            try {
                Files.createDirectory(outputPath);
            } catch (IOException ex) {
                String errorMessage = "Unable to create an output directory on path " + OUTPUT_PATH_DIR_NAME;
                log.error(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
        }
        return outputPath;
    }
}

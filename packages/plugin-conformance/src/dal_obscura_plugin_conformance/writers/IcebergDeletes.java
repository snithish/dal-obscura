import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;

/** Native delete files, manifests and snapshots; only catalog publication crosses Python. */
public final class IcebergDeletes {
  private static final ObjectMapper JSON = new ObjectMapper();

  private static final class Operations implements TableOperations {
    private TableMetadata metadata;
    private final FileIO io;
    Operations(String location, FileIO io) {
      this.io = io;
      this.metadata = TableMetadataParser.read(io, location);
    }
    public TableMetadata current() { return metadata; }
    public TableMetadata refresh() { return metadata; }
    public FileIO io() { return io; }
    public String metadataFileLocation(String name) { return metadata.location() + "/metadata/" + name; }
    public org.apache.iceberg.io.LocationProvider locationProvider() {
      return LocationProviders.locationsFor(metadata.location(), metadata.properties());
    }
    public void commit(TableMetadata base, TableMetadata updated) {
      if (base != metadata) throw new IllegalStateException("Fixture metadata changed");
      String location = metadataFileLocation(UUID.randomUUID() + ".metadata.json");
      TableMetadataParser.write(updated, io.newOutputFile(location));
      metadata = TableMetadataParser.read(io, location);
    }
  }

  private static Object scalar(JsonNode value, Types.NestedField field) {
    if (value.isNull()) return null;
    return switch (field.type().typeId()) {
      case INTEGER -> value.intValue();
      case LONG -> value.longValue();
      case FLOAT -> value.floatValue();
      case DOUBLE -> value.doubleValue();
      case BOOLEAN -> value.booleanValue();
      case STRING -> value.textValue();
      default -> throw new IllegalArgumentException("Unsupported fixture equality type: " + field.type());
    };
  }

  public static void main(String[] args) throws Exception {
    JsonNode request = JSON.readTree(Files.readString(Path.of(args[0])));
    String location = request.get("metadata_location").asText();
    FileIO io;
    if (location.startsWith("s3://")) {
      S3FileIO s3 = new S3FileIO();
      Map<String, String> properties = new HashMap<>();
      String endpoint = System.getenv("AWS_ENDPOINT_URL");
      if (endpoint != null) properties.put("s3.endpoint", endpoint);
      properties.put("s3.path-style-access", "true");
      properties.put("client.region", System.getenv().getOrDefault("AWS_DEFAULT_REGION", "us-east-1"));
      s3.initialize(properties);
      io = s3;
    } else {
      io = new HadoopFileIO(new Configuration());
    }
    try (io) {
      Operations operations = new Operations(location, io);
      Table table = new BaseTable(operations, "fixture");
      String deletePath = request.get("delete_path").asText();
      GenericRecord partition = GenericRecord.create(table.spec().partitionType());
      JsonNode rawPartition = request.get("partition");
      for (int i = 0; i < rawPartition.size(); i++) {
        partition.set(i, scalar(rawPartition.get(i), table.spec().partitionType().fields().get(i)));
      }
      DeleteFile file;
      if (request.get("kind").asText().equals("equality")) {
        String[] names = JSON.convertValue(request.get("equality_columns"), String[].class);
        Schema equality = table.schema().select(names);
        int[] ids = equality.columns().stream().mapToInt(Types.NestedField::fieldId).toArray();
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec(), ids, equality, null);
        var writer = factory.newEqDeleteWriter(EncryptedFiles.encryptedOutput(io.newOutputFile(deletePath), (java.nio.ByteBuffer) null), FileFormat.PARQUET, partition);
        try (writer) {
          for (JsonNode values : request.get("records")) {
            GenericRecord record = GenericRecord.create(equality);
            for (Types.NestedField field : equality.columns()) record.setField(field.name(), scalar(values.get(field.name()), field));
            writer.write(record);
          }
        }
        file = writer.toDeleteFile();
      } else {
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());
        var writer = factory.newPosDeleteWriter(EncryptedFiles.encryptedOutput(io.newOutputFile(deletePath), (java.nio.ByteBuffer) null), FileFormat.PARQUET, partition);
        try (writer) {
          for (JsonNode values : request.get("records")) {
            writer.write(PositionDelete.<org.apache.iceberg.data.Record>create().set(values.get("file_path").asText(), values.get("pos").asLong(), null));
          }
        }
        file = writer.toDeleteFile();
      }
      table.newRowDelta().addDeletes(file).commit();
      Files.writeString(Path.of(args[1]), TableMetadataParser.toJson(operations.current()));
    }
  }
}

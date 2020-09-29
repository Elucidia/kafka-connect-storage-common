package io.confluent.connect.storage.partitioner;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class CustomPartitioner<T> extends DefaultPartitioner<T> {
    private static final Logger log = LoggerFactory.getLogger(CustomPartitioner.class);
    private List<String> fieldNames;
    private List<SinkRecordParser> parsers;
    private Pattern pathPattern;
    private String path;
    private String fileDelim;
    private String dirDelim;
    private String topicsDir;

    private static final String TOPICS_DIRECTORY = "topicsDir";
    private static final String DIRECTORY_DELIMITER = "dirDeli";
    private static final String FILE_DELIMITER = "fileDeli";
    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";
    private static final String OFFSET = "offset";

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> config) {
        pathPattern = Pattern.compile("\\$\\{([^}\\s]+)}");
        path = (String) config.get(PartitionerConfig.PARTITION_CUSTOM_PATH_CONFIG);

        List<FieldConfig> fields = parseFieldConfigs((List<String>) config.get(PartitionerConfig.PARTITION_CUSTOM_FIELDS_CONFIG));
        parsers = getSinkRecordParsers(fields);
        fieldNames = fields
                        .stream()
                        .map(FieldConfig::getPath)
                        .collect(Collectors.toList());

        dirDelim = (String) config.get(StorageCommonConfig.DIRECTORY_DELIM_CONFIG);
        fileDelim = (String) config.get(StorageCommonConfig.FILE_DELIM_CONFIG);
        topicsDir = (String) config.get(StorageCommonConfig.TOPICS_DIR_CONFIG);
    }

    static protected List<FieldConfig> parseFieldConfigs(List<String> fieldConfigs) {
        List<FieldConfig> fields = new ArrayList<>();
        for (String fieldConfig: fieldConfigs) {
            fields.add(new FieldConfig(fieldConfig));
        }
        return fields;
    }

    static protected List<SinkRecordParser> getSinkRecordParsers(List<FieldConfig> fields) {
        List<SinkRecordParser> parsers = new ArrayList<>();
        for(FieldConfig field: fields) {
            if (field.isKey()) {
                parsers.add(new SinkRecordKeyParser(field));
            } else {
                parsers.add(new SinkRecordValueParser(field));
            }
        }
        return parsers;
    }

    protected Map<String, String> getDefaultMapping(String topic, Integer partition, long offset) {
        HashMap<String, String> map = new HashMap<>();
        map.put(TOPICS_DIRECTORY, topicsDir);
        map.put(DIRECTORY_DELIMITER, dirDelim);
        map.put(FILE_DELIMITER, fileDelim);
        map.put(TOPIC, topic);
        map.put(PARTITION, partition.toString());
        map.put(OFFSET, String.format("%010d", offset));
        return map;
    }

    protected Map<String, String> getMapping(SinkRecord sinkRecord) {
        Map<String, String> map = getDefaultMapping(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
        for(SinkRecordParser parser: parsers) {
            String parsedValue = parser.Parse(sinkRecord);
            map.put(parser.getField().getName(), parsedValue);
        }
        return map;
    }

    protected String parsePath(SinkRecord sinkRecord) {
        Map<String, String> map = getMapping(sinkRecord);

        Matcher m = pathPattern.matcher(path);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(sb, map.get(m.group(1)));
        }
        m.appendTail(sb);

        return sb.toString();
    }

    public boolean encodedPartitionIsFullPath() {
        return true;
    }

    @Override
    public String encodePartition(SinkRecord sinkRecord) {
        String encoded = parsePath(sinkRecord);
        log.info(encoded);
        return encoded;
    }

    @Override
    public List<T> partitionFields() {
        if (partitionFields == null) {
            partitionFields = newSchemaGenerator(config).newPartitionFields(
                Utils.join(fieldNames, ",")
            );
        }
        return partitionFields;
    }
}

class FieldConfig {
    private String name;
    private String path;
    private boolean isKey;

    private static String NAME_DELIMITER = ":";
    private static String KEY_VALUE_DELIMITER = "$";

    public FieldConfig(String fieldConfig) {
        String[] temp = fieldConfig.split(NAME_DELIMITER);
        name = temp[1];
        temp = temp[0].split(KEY_VALUE_DELIMITER);
        path = temp[0];
        isKey = temp[1].toLowerCase().equals("key");
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    public boolean isKey() {
        return isKey;
    }
}

abstract class SinkRecordParser {
    private List<String> fieldNames;
    private FieldConfig field;
    private static final Logger log = LoggerFactory.getLogger(SinkRecordParser.class);
    protected static final String PURPOSE = "Partitioning topic into s3 with key value";

    public SinkRecordParser(FieldConfig field) {
        this.field = field;
        fieldNames = Arrays.asList(field.getPath().split("\\."));
    }

    protected abstract Map<String, Object> getSinkRecordMap(SinkRecord sinkRecord);

    public FieldConfig getField() {
        return field;
    }

    public String Parse(SinkRecord sinkRecord) {
        Object value = sinkRecord.value();
        log.info(value.toString());

        Map<String, Object> map = getSinkRecordMap(sinkRecord);
        log.info(map.toString());

        Iterator<String> iterator = fieldNames.iterator();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            if (!iterator.hasNext()) {
                String fieldValue = map.get(fieldName).toString();
                log.info(fieldValue);
                return fieldValue;
            }
            map = requireMap(map.get(fieldName), PURPOSE);
        }

        throw new PartitionException("Invalid path: " + field.getPath());
    }
}

class SinkRecordKeyParser extends SinkRecordParser {
    public SinkRecordKeyParser(FieldConfig field) {
        super(field);
    }

    @Override
    protected Map<String, Object> getSinkRecordMap(SinkRecord sinkRecord) {
        return requireMap(sinkRecord.key(), PURPOSE);
    }
}

class SinkRecordValueParser extends SinkRecordParser {
    public SinkRecordValueParser(FieldConfig field) {
        super(field);
    }

    @Override
    protected Map<String, Object> getSinkRecordMap(SinkRecord sinkRecord) {
        return requireMap(sinkRecord.value(), PURPOSE);
    }
}

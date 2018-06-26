package ndr.brt.mybroker.server;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import ndr.brt.mybroker.protocol.ConsumerRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Long.valueOf;

public class InMemoryStore implements Store {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, List<byte[]>> topics = new ConcurrentHashMap<>();

    @Override
    public long write(String topic, byte[] data) {
        List<byte[]> records = topics.getOrDefault(topic, new ArrayList<>());
        records.add(data);
        topics.put(topic, records);
        return records.size();
    }

    @Override
    public List<ConsumerRecord> read(String topic, long offset) {
        logger.info("Read requested for topic " + topic + " and offset " + offset);
        List<byte[]> records = topics.getOrDefault(topic, new ArrayList<>());

        return IntStream.range(valueOf(offset + 1).intValue(), records.size())
                .mapToObj(i -> new ConsumerRecord(i, records.get(i)))
                .collect(Collectors.toList());
    }
}

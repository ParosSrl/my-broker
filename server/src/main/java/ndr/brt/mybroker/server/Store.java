package ndr.brt.mybroker.server;

import ndr.brt.mybroker.protocol.ConsumerRecord;

import java.util.List;

public interface Store {

    long write(String topic, byte[] data);

    List<ConsumerRecord> read(String topic, long offset);
}

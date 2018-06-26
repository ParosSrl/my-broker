package ndr.brt.mybroker.protocol.response;

import ndr.brt.mybroker.protocol.ConsumerRecord;
import ndr.brt.mybroker.protocol.Header;

import java.util.List;

public class Consumed extends Response {

    private final List<ConsumerRecord> records;

    public Consumed(Header header, List<ConsumerRecord> records) {
        super(header);
        this.records = records;
    }

    public List<ConsumerRecord> records() {
        return records;
    }
}

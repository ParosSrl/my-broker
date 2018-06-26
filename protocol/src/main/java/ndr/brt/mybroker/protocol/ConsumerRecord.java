package ndr.brt.mybroker.protocol;

public class ConsumerRecord {
    private final long offset;
    private final byte[] value;

    public ConsumerRecord(long offset, byte[] value) {
        this.offset = offset;
        this.value = value;
    }
}

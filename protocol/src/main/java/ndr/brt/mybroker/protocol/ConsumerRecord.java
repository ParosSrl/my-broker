package ndr.brt.mybroker.protocol;

import java.io.Serializable;
import java.util.Arrays;

public class ConsumerRecord implements Serializable {
    private final long offset;
    private final byte[] value;

    public ConsumerRecord(long offset, byte[] value) {
        this.offset = offset;
        this.value = value;
    }

    public Long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return "ConsumerRecord{" +
                "offset=" + offset +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}

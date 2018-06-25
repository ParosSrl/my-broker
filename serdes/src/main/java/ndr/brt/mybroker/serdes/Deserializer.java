package ndr.brt.mybroker.serdes;

public interface Deserializer<T> {
    T deserialize(byte[] value);
}

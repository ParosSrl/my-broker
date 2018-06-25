package ndr.brt.mybroker.serdes;

public interface SerDes<T> {
    byte[] serialize(T object);
    T deserialize(byte[] data);
}

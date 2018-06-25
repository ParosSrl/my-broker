package ndr.brt.mybroker.serdes;

public interface Serializer<T> {
    byte[] serialize(T object);
}

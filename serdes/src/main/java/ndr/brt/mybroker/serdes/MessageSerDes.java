package ndr.brt.mybroker.serdes;

import ndr.brt.mybroker.protocol.Message;

public class MessageSerDes implements SerDes<Message> {

    private final Serializer<Message> serializer = new MessageSerializer();
    private final Deserializer<Message> deserializer = new MessageDeserializer();

    public byte[] serialize(Message object) {
        return serializer.serialize(object);
    }

    public Message deserialize(byte[] data) {
        return deserializer.deserialize(data);
    }
}

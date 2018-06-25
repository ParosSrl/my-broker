package ndr.brt.mybroker.serdes;

import ndr.brt.mybroker.protocol.Message;

public class MessageSerializer implements Serializer<Message> {
    public byte[] serialize(Message object) {
        return new byte[0];
    }
}

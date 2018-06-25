package ndr.brt.mybroker.serdes;

import ndr.brt.mybroker.protocol.Message;

public class MessageSerDes implements SerDes<Message> {
    public byte[] serialize(Message object) {
        return new byte[0];
    }

    public Message deserialize(byte[] data) {
        return null;
    }
}

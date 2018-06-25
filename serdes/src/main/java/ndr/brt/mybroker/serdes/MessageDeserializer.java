package ndr.brt.mybroker.serdes;

import ndr.brt.mybroker.protocol.Message;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;

public class MessageDeserializer implements Deserializer<Message> {
    public Message deserialize(byte[] value) {
        try (
            final ByteArrayInputStream bis = new ByteArrayInputStream(value);
            ObjectInput in = new ObjectInputStream(bis)
        ){
            return (Message) in.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}

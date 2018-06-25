package ndr.brt.mybroker.serdes;

import ndr.brt.mybroker.protocol.Message;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class MessageSerializer implements Serializer<Message> {

    public byte[] serialize(Message object) {
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            final ObjectOutput output = new ObjectOutputStream(stream);
            output.writeObject(object);
            output.flush();
            return stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}

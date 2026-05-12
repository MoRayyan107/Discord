package manager;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import stage3.dm.DirectMessageInterface;
import stage3.dm.DirectServerMessage;
import stage3.groupchat.interfaces.ChatParticipant;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class KafkaManager {

    private final KafkaProducer<String, String> producer;
    private final List<ChatParticipant> participants;
    private final DirectMessageInterface dmInterface;

    public KafkaManager(List<ChatParticipant> chats, DirectMessageInterface dmInterface) {
        try{
            Properties props = new Properties();
            InputStream propertiesStream = KafkaManager.class.getClassLoader().getResourceAsStream("producer.properties");
            props.load(propertiesStream);
            producer = new KafkaProducer<>(props);

            participants = chats;
            this.dmInterface = dmInterface;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void sendMessageToKafka(String topic, String message){
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending message to Kafka: " + exception.getMessage());
            }
        });
    }

    public void deliverLKafkaMessageGroup(String senderUsername, String groupName, String message){
        String formattedMessage = senderUsername + ": " + message;
        for (ChatParticipant participant : participants) {
            if (groupName.equals(participant.getCurrentGroup())) {
                if (!participant.getUsername().equals(senderUsername)) {
                    participant.sendToClient(formattedMessage);
                }
            }
        }
    }

    public void deliverLKafkaDM(String sender, String receiver, String message){
        ChatParticipant receiverParticipant = findUserInChats(receiver);

        if (receiverParticipant != null) {
            if (!dmInterface.checkIfDirectMessageExists(sender, receiver)){
                System.out.println("Direct message session does not exist between " + sender + " and " + receiver + "; creating one.");
                dmInterface.createDirectMessage(sender, receiver);
            }

            // Use the sender string for formatting even if the sender isn't local
            String formattedMessage = sender + ": " + message;
            receiverParticipant.sendToClient(formattedMessage);
        } else {
            System.err.println("Receiver not found in chat: " + receiver);
        }

    }


    private ChatParticipant findUserInChats(String senderUsername){
        for (ChatParticipant participant : participants) {
            if (participant.getUsername().equals(senderUsername))
                return participant;
        }
        return null; // User not found
    }


}

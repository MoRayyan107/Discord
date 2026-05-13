import manager.KafkaManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import stage3.dm.DirectMessageInterface;
import stage3.groupchat.interfaces.ChatParticipant;

import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ClientHandlerTest {

    @AfterEach
    public void tearDownClass() {
        ClientHandler.getClients().clear();
    }

    @Test
    public void testSendMessageInCrosServer_Group_PASS(){
        // Arrange
        ChatParticipant memberInGroup = mock(ChatParticipant.class);
        when(memberInGroup.getCurrentGroup()).thenReturn("group1");
        when(memberInGroup.getUsername()).thenReturn("Bob");

        ChatParticipant sender = mock(ChatParticipant.class);
        when(sender.getCurrentGroup()).thenReturn("group1");
        when(sender.getUsername()).thenReturn("Alice");

        ClientHandler.getClients().add(memberInGroup);
        ClientHandler.getClients().add(sender);

        // Act
        ClientHandler.deliverKafkaMessageGroup("Alice:group1:Hello Group!");

        // ASSERT
        verify(memberInGroup,times(1)).getCurrentGroup();
        verify(memberInGroup).sendToClient("Alice: Hello Group!");
        verify(sender,never()).sendToClient(anyString());
    }

    @Test
    public void testSendMessageInCrosServer_Group_FAIL(){
        ChatParticipant senderNotInGroup = mock(ChatParticipant.class);
        ChatParticipant receiver = mock(ChatParticipant.class);
        // get Username and getGroup will never be called when the sender is not in any grp

        ClientHandler.getClients().add(senderNotInGroup);
        ClientHandler.getClients().add(receiver);

        // ACT
        ClientHandler.deliverKafkaMessageGroup("Bob::Hello Group!"); // Bob isnt in any group (null)

        // ASSERT
        verifyNoInteractions(senderNotInGroup, receiver);
        verify(senderNotInGroup, never()).getCurrentGroup();
        verify(senderNotInGroup, never()).getUsername();
    }

    @Test
    public void testSendDirectMessageInCrosServer_ReceiverNotNull_PASS(){
        // ARRANGE
        ChatParticipant sender = mock(ChatParticipant.class);
        when(sender.getUsername()).thenReturn("Alice");

        ChatParticipant receiver = mock(ChatParticipant.class);
        when(receiver.getUsername()).thenReturn("Bob");

        ClientHandler.getClients().add(sender);
        ClientHandler.getClients().add(receiver);

        // ACT
        ClientHandler.deliverLKafkaMessageDM("Alice:Bob:Hello Group!");

        // VERIFY
        verify(receiver, times(1)).getUsername();
        verify(sender, never()).sendToClient("Alice: Hello Group!");
    }

    @Test
    public void testSendDirectMessageInCrosSever_ReceiverNull_FAIL(){
        // ARRANGE
        ChatParticipant sender = mock(ChatParticipant.class);
        ChatParticipant receiver = mock(ChatParticipant.class);
        // no need for a return since itll not call getUsername() at all

        ClientHandler.getClients().add(sender);
        ClientHandler.getClients().add(receiver);

        // ACT
        ClientHandler.deliverLKafkaMessageDM("Alice::Hello Bob");

        // VERIFY
        verify(receiver,never()).getUsername();
        verify(sender,never()).sendToClient("Alice: Hello Bob");
    }

    @Test
    public void testSendMessageToKafka_PASS(){
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        List<ChatParticipant> clients = ClientHandler.getClients();
        DirectMessageInterface dmInterface = mock(DirectMessageInterface.class);

        KafkaManager km = new KafkaManager(clients, dmInterface, producer);
        km.sendMessageToKafka("topic1", "Hello Kafka");

        verify(producer).send(argThat(record ->
                "topic1".equals(record.topic()) &&
                "Hello Kafka".equals(record.value())
        ), any());
    }

    // TODO: WRITE SOME MORE TEST
}

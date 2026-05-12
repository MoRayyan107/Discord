import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import stage3.groupchat.interfaces.ChatParticipant;

import java.util.ArrayList;
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
        System.out.println("Message Sent to group members except sender");
    }

    // TODO: TESTS MORE PLEASE
//    @Test
//    public void testSendMessageInCrosServer_Group_FAIL(){
//        ChatParticipant senderNotInGroup = mock(ChatParticipant.class);
//        when(senderNotInGroup.getCurrentGroup()).thenReturn(null);
//        when(senderNotInGroup.getUsername()).thenReturn("Bob");
//
//        ChatParticipant receiver = mock(ChatParticipant.class);
//        when(receiver.getCurrentGroup()).thenReturn("group1");
//        when(receiver.getUsername()).thenReturn("Alice");
//
//        ClientHandler.getClients().add(senderNotInGroup);
//        ClientHandler.getClients().add(receiver);
//
//        // ACT
//        ClientHandler.deliverKafkaMessageGroup("Bob:group1:Hello Group!");
//
//        // ASSERT
//        verify(senderNotInGroup, never()).sendToClient("Bob: Hello Group!");
//        verify(receiver, never()).sendToClient(any());
//        System.out.println("Message not sent to group members because sender is not in the group");
//    }
}

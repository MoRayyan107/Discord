import manager.KafkaManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import stage3.dm.DirectMessageInterface;
import stage3.groupchat.interfaces.ChatParticipant;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaManagerTest {

    private List<ChatParticipant> participants;
    private DirectMessageInterface dmInterface;
    private KafkaManager kafkaManager;

    @BeforeEach
    void setUp() {
        participants = new ArrayList<>();
        dmInterface = mock(DirectMessageInterface.class);
        kafkaManager = new KafkaManager(participants, dmInterface);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeProducer(kafkaManager);
        participants.clear();
        Mockito.reset(dmInterface);
        kafkaManager = null;
    }

    @Test
    void deliverLKafkaMessageGroup_sendsOnlyToSameGroupMembersExceptSender() {
        ChatParticipant sender = participant("alice", "study");
        ChatParticipant receiver = participant("bob", "study");
        ChatParticipant outsider = participant("carol", null);
        participants.add(sender);
        participants.add(receiver);
        participants.add(outsider);

        kafkaManager.deliverLKafkaMessageGroup("alice", "study", "hello group");

        verify(receiver).sendToClient("alice: hello group");
        verify(sender, never()).sendToClient("alice: hello group");
        verify(outsider, never()).sendToClient("alice: hello group");
    }

    @Test
    void deliverLKafkaDM_sendsToLocalReceiverAndCreatesSessionWhenMissing() {
        ChatParticipant receiver = participant("bob", null);
        participants.add(receiver);
        when(dmInterface.checkIfDirectMessageExists("alice", "bob")).thenReturn(false);
        when(dmInterface.createDirectMessage("alice", "bob")).thenReturn(true);

        kafkaManager.deliverLKafkaDM("alice", "bob", "hello dm");

        verify(dmInterface).checkIfDirectMessageExists("alice", "bob");
        verify(dmInterface).createDirectMessage("alice", "bob");
        verify(receiver).sendToClient("alice: hello dm");
    }

    @Test
    void deliverLKafkaDM_doesNotCreateSessionWhenItAlreadyExists() {
        ChatParticipant receiver = participant("bob", null);
        participants.add(receiver);
        when(dmInterface.checkIfDirectMessageExists("alice", "bob")).thenReturn(true);

        kafkaManager.deliverLKafkaDM("alice", "bob", "still here");

        verify(dmInterface).checkIfDirectMessageExists("alice", "bob");
        verify(dmInterface, never()).createDirectMessage("alice", "bob");
        verify(receiver).sendToClient("alice: still here");
    }

    private ChatParticipant participant(String username, String currentGroup) {
        ChatParticipant participant = mock(ChatParticipant.class);
        when(participant.getUsername()).thenReturn(username);
        when(participant.getCurrentGroup()).thenReturn(currentGroup);
        return participant;
    }

    private void closeProducer(KafkaManager manager) {
        try {
            Field field = KafkaManager.class.getDeclaredField("producer");
            field.setAccessible(true);
            Object producer = field.get(manager);
            if (producer instanceof KafkaProducer<?, ?> kafkaProducer) {
                kafkaProducer.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to close KafkaProducer during test teardown", e);
        }
    }
}


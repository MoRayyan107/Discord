import java.io.*;
import java.net.Socket;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// interfaces
import manager.KafkaManager;
import manager.RedisManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stage3.dm.DirectMessageInterface;
import stage3.dm.DirectServerMessage;
import stage3.status.StatusInterface;
import stage3.friend.FriendInterface;
import stage3.messagehistory.MessageHistory;
import stage3.groupchat.interfaces.ChatParticipant;
import stage3.groupchat.interfaces.GroupChatInterfaces;

// imp stuff
import manager.UsersDB;

// Safe and Unsafe Imports
import stage3.status.SafeStatus;  // SAFE VERSION OF STATUS
import stage3.friend.SafeFriendManager;
import stage3.groupchat.SafeGroupChat;
import stage3.messagehistory.SafeMessageHistory;

/**
 * Handles each connected client on its own thread.
 * Responsible for reading client input, parsing commands,
 * and routing messages to the correct destination.
 *
 * @version 1.3
 */
public class ClientHandler implements Runnable, ChatParticipant {

    private Socket socket;
    private BufferedReader in;
    private BufferedWriter out;
    private String username;
    private final String clientIP;


    // rate limit checks
    private int messageCountForRateLimit = 0;
    private Long lastMessageTime = System.currentTimeMillis();

    // volatile ensures thread safety for each thread for a user group
    private volatile String currentGroup;

    private static final List<ChatParticipant> clients = Collections.synchronizedList(new ArrayList<>());
    private static final Map<String, GroupChatInterfaces> groupChats = new ConcurrentHashMap<>();

    // SAFE VERSIONS
    private static final StatusInterface statusService = new SafeStatus();
    private static final MessageHistory messageHistory = new SafeMessageHistory();
    private static final FriendInterface friendService = new SafeFriendManager();
    private static final DirectMessageInterface directMessageService = new DirectServerMessage();

    // Logger FACTORY
    private static final Logger log = LoggerFactory.getLogger(ClientHandler.class);

    // ---------------------- ALL STATIC INITIALISATION -----------------------------

    // DB initialising
    private final static UsersDB usersDB; // for database interactions
    static { // to handel Checked exceptions
        try{
            usersDB = new UsersDB();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static final KafkaManager kafkaManager;
    static {
        try{
            kafkaManager = new KafkaManager(clients, directMessageService);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Kafkan producere
    private static final KafkaProducer<String, String> producer;
    static {
        try{
            Properties props = new Properties();
            InputStream propertiesStream = ClientHandler.class.getClassLoader().getResourceAsStream("producer.properties");
            props.load(propertiesStream);
            producer = new KafkaProducer<>(props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Redis static block
    private static final RedisManager redisManager;
    static{
        try{
            redisManager = new RedisManager();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Makes a new Client object for each client thats connected to the server, and adds it to the clients list
     * at end it sends the message to server and the connected clients
     *
     * @param socket the socket connected to the client
     * @throws IOException
     */
    public ClientHandler(Socket socket) throws IOException {
        this.socket = socket;
        this.clientIP = socket.getInetAddress().getHostAddress();

        try{
            this.in = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), "UTF-8"));
            this.out = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream(), "UTF-8"));

            int choice = Integer.parseInt(this.in.readLine());
            this.username = in.readLine();

            if (choice == 1) {
                String password = this.in.readLine();
                if (authenticate(username, password)) {
                    out.write("AUTH_SUCCESS\n");
                    out.flush();
                    clients.add(this);
                    redisManager.setUserServer(username, Server.getServerID());
                    statusService.userJoined(username);
                    friendService.initUser(username);
                } else {
                    out.write("AUTH_FAILURE\n");
                    out.flush();
                    sendToClient("Authentication failed. Please check your username and password.");
                    closeConnections(socket, out, in);
                }
            }

            else if (choice == 2) {
                String password = this.in.readLine();
                if (userRegister(username, password, in.readLine())) {
                    out.write("REG_SUCCESS\n");
                    out.flush();
                    clients.add(this);
                    redisManager.setUserServer(username, Server.getServerID());
                    statusService.userJoined(username);
                    friendService.initUser(username);
                } else {
                    out.write("REG_FAILURE\n");
                    out.flush();
                    sendToClient("Registration failed. Please check your username and password.");
                    closeConnections(socket, out, in);
                }
            }

            else{
                sendToClient("Invalid choice. Connection closed.");
                closeConnections(socket, out, in);
            }

        } catch (Exception e) {
            sendToClient("An error occurred during authentication/registration: " + e.getMessage());
            closeConnections(socket, out, in);
        }
    }

    // DB functions
    private boolean authenticate(String username, String password) throws SQLException {
        if (username == null || password == null) return false;

        return usersDB.login(username, password);
    }

    private boolean userRegister(String username, String password, String email) throws SQLException {
        if  (username == null || password == null || email == null) return false;

        return usersDB.register(username, email, password);
    }

    // -------- getter function ----------
    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getClientIP(){
        return clientIP;
    }

    // can return null
    @Override
    public String getCurrentGroup() {
        return currentGroup;
    }

    // used for testing
    public static List<ChatParticipant> getClients() { return clients; }

    // --------------- HELPER FUNCTION ----------------
    private ChatParticipant getUserFromChat(String targetUsername){
        ChatParticipant target = null;
        synchronized (clients) {
            for (ChatParticipant client : clients) {
                if (client.getUsername().equalsIgnoreCase(targetUsername)) {
                    target = client;
                    break;
                }
            }
        }
        return target;
    }




    /**
     * Sends a message directly to this client only.
     * @param message the message to deliver
     */
    @Override
    public void sendToClient(String message) {
        try {
            out.write(message + "\n");
            out.flush();
        } catch (IOException e) {
            System.out.println("Error sending message to " + username + ": " + e.getMessage());
        }
    }

    /**
     * Main loop — reads messages from the client and routes them.
     * listens for commands for making any groups oe any necessity things
     */
    @Override
    public void run() {
        String messageToSend;

        while(this.socket.isConnected()) {
            try {
                messageToSend = in.readLine();

                if (messageToSend == null) {
                    System.out.println("Client " + username + " has been disconnected."); // send to server
                    broadcastMessage("SERVER: " + username + " has been disconnected.");
                    break; // client disconnected
                }
                if (messageToSend.startsWith("!")) {
                    handleCommands(messageToSend);
                } else {
                    handleBroadcast(messageToSend);
                }
            } catch (IOException e) {
                System.out.println("Error reading from " + username + ": " + e.getMessage());
                break;
            }
        }
        closeConnections(socket, out, in);
    }


    /**
     * This is a helper function to handle the creation of a new group chat.
     * It checks if the group name already exists, if not it creates a new group chat and adds the user to it.
     *
     * @param groupName the name of the Group chat to create
     */
    private void handleCreate(String groupName) {
        groupChats.putIfAbsent(groupName, new SafeGroupChat(groupName));
        this.currentGroup = groupName;
        String ServerID = Server.getServerID();

        GroupChatInterfaces chat = groupChats.get(groupName);
        chat.addMember(this);
        redisManager.createGroup(groupName, ServerID);
        redisManager.joinGroup(groupName, username, ServerID);
        sendToClient("SERVER: Group '" + groupName + "' created.");
    }


    private void handleJoin(String groupName) {
        GroupChatInterfaces chat = groupChats.get(groupName);

        if (chat != null){
            this.currentGroup = groupName; // we wanna remember the users group
            chat.addMember(this);
            sendToClient("SERVER: You joined '" + groupName + "'.");

            List<String> history = chat.getMessagesHistory();
            if (!history.isEmpty()) {
                sendToClient("SERVER: --- " + history.size() + " previous message(s) ---");
                for (String msg : history) {
                    sendToClient(msg);
                }
            }
        } else if (redisManager.groupExists(groupName)) {
            redisManager.joinGroup(groupName, username, Server.getServerID());
            this.currentGroup = groupName;
            sendToClient("SERVER: Joined '" + groupName + "'.");
        } else {
            sendToClient("SERVER: Group '" + groupName + "' not found.");
        }
    }

    /**
     * This is a helper function where we can directly send a message to a group without even joining it,
     * it checks if the group exists and sends the message to that group, if not it sends an error message to the client
     *
     * @param groupName Target Group name
     * @param messgae the message to send
     */
    private void handleGroupMessage(String groupName, String messgae) {

        GroupChatInterfaces chat = groupChats.get(groupName);
        if (chat == null) {
            sendToClient("SERVER: Group '" + groupName + "' not found.");
            return;
        }

        chat.sendMessage(this, messgae);
    }

    /**
     * This is a helper function where a User can leave a group,
     * It checks if the user is in a group,
     * if they are it removes them from the group and sends a message to the server and the group members,
     * if not it sends an error message to the client
     */
    private void handleLeaveGroup(){
        if (currentGroup != null) {
            GroupChatInterfaces chat = groupChats.get(currentGroup);
            if (chat != null) {
                chat.removeMember(this);
            }
            String uSerServer = Server.getServerID();
            redisManager.leaveGroup(currentGroup, username, uSerServer);
            consoleDisplay("You left the group '" + currentGroup + "'.");
            currentGroup = null;
        } else{
            consoleDisplay("You are not currently in a group.");
        }
    }

    /**
     * Handles the file sending process by coordinating between the sender and receiver.
     * gets the Receivers IP and generates a random Port number
     * and creates a secure line transfer files
     *
     * @param targetUsername the user to send file to
     */
    private void handleSendFile( String targetUsername){
        ChatParticipant target = getUserFromChat(targetUsername);

        if(target == null){
            consoleDisplay("User " + targetUsername + " not found.");
            return;
        }

        String receiverIP = target.getClientIP();
        int randPort = 4000 + (int)(Math.random() * 6000); // 4000 - 9999

        target.sendToClient("FILE_RECEIVER_READY:"+randPort+ ":"+ this.username);

        this.sendToClient("FILE_SENDER_READY:"+receiverIP+ ":"+ randPort);
    }

    /**
     * a helper function  handling the video streaming process by coordinating between the sender and receiver.
     * gets the Receivers IP and generates a random Port number
     * and creates a secure line to stream videos
     *
     * @param targetUsername Sender's username
     */
    private void handleStream(String targetUsername) {
        ChatParticipant target = getUserFromChat(targetUsername);

        if (target == null) {
            consoleDisplay("User " + targetUsername + " not found.");
            return;
        }

        String receiverIP = target.getClientIP();
        int randPort = 4000 + (int)(Math.random() * 6000); // 4000-9999

        target.sendToClient("STREAM_RECEIVER_READY:" + randPort + ":" + this.username);
        this.sendToClient("STREAM_SENDER_READY:" + receiverIP + ":" + randPort);
    }

    /**  If yall having Confusion on why we have two broadcast methods, here is the difference:
     * The Diffrence between handleBroadcast and broadcastMessage is that, handleBroadcast is for when a client sends a message to the server,
     * the server needs to broadcast it to everyone else and adds into the groupchat history,
     * while broadcastMessage is for when the server needs to send a message
     * to everyone else (like when a client joins or leaves)
     */

    private void handleBroadcast(String msgToSend) {

        if (messageCountForRateLimit > 5 && lastMessageTime + 10000 >= System.currentTimeMillis()) {
            consoleDisplay("CANNOT SEND MESSAGE RATE LIMIT HOT WAIT FOR " + ((lastMessageTime + 10000 - System.currentTimeMillis()) / 1000) + " SECONDS");
            return;
        }

        // reset the rate limit if 10 seconds have passed since the last message was sent
        if (System.currentTimeMillis() - lastMessageTime > 10000) {
            messageCountForRateLimit = 0;
            lastMessageTime = System.currentTimeMillis();
        }

        // only add to message History if current group exists
        if (currentGroup != null) {
            messageHistory.addMessage(msgToSend);
        }

        messageCountForRateLimit++;

        // Group message goes -> ServerId#username:groupName:Message
        String grpName = (currentGroup == null) ? "" : currentGroup;
        String payloadTosend = Server.getServerID() + "#" + username + ":" + grpName + ":" + msgToSend;

        // so here we check if thers any other users with diff server if yes then use this
        if (currentGroup != null && redisManager.isGroupCrossServer(grpName, Server.getServerID()))
            kafkaManager.sendMessageToKafka("messages", payloadTosend);

        broadcastMessage(msgToSend);
    }

    public void broadcastMessage(String message) {
        if (currentGroup != null) {
            GroupChatInterfaces chat = groupChats.get(currentGroup);
            if (chat != null) {
                chat.sendMessage(this, message);
            } else {
                if (!redisManager.groupExists(currentGroup)) currentGroup=null;
            }
        } else {
            // send message ii general chat if that user has not joined the group
            synchronized (clients) {
                for (ChatParticipant client : clients) {

                    // we dont wanna send to group
                    if (!client.getUsername().equals(this.username) && client.getCurrentGroup() == null) {
                        client.sendToClient(this.username + ": " + message);
                    }
                }
            }
        }
    }

    public void handelDM(String receiver, String messageToSend){
        // First check if the target user is in sender's server
        ChatParticipant target = getUserFromChat(receiver);

        if (target != null) {
            // Receiver is local, deliver directly
            target.sendToClient("(DM) " + this.username + ": " + messageToSend);
        } else {
            // Receiver is not local, check Redis for their server
            String receiverServerID = redisManager.getUserServer(receiver);
            
            if (receiverServerID != null && !receiverServerID.isEmpty()) {
                // Receiver is on another server, send targeted Kafka message
                String payloadToSend = receiverServerID + "#" + this.username + ":" + receiver + ":" + messageToSend;
                kafkaManager.sendMessageToKafka("direct_message", payloadToSend);
            } else {
                // Receiver not found on any server
                consoleDisplay("User " + receiver + " is not currently online.");
            }
        }
    }

    // ----------------------------------------- Kafka Functions --------------------------------------

    public static void deliverLKafkaMessageDM(String chunk){
        String[] divided = chunk.split(":",3);
        String sender = divided[0];
        String receiver = divided[1];
        String message = divided[2];

        kafkaManager.deliverLKafkaDM(sender, receiver, message);
    }

    public static void deliverKafkaMessageGroup(String chunk) {
        String[] divided = chunk.split(":",3);
        String senderUsername = divided[0];
        String groupName = divided[1].isEmpty() ? null : divided[1]; // since most of the functions handel grp name as null
        String message = divided[2];

        log.info("Sending: {}",chunk);

        kafkaManager.deliverLKafkaMessageGroup(senderUsername, groupName, message);
    }



    /**
     * Handles the command prompt by the User,
     * it checks the command and executes the corresponding function,
     * if the command is not recognized it sends an error message to the client
     *
     * @param command Command to execute
     */
    public void handleCommands(String command) {
        String[] args = command.split(" ",3);
        String commandKey = args[0].toLowerCase();

        // args -> [command, [text/group/username, [message]]]] [NOT A list(list(list))) ]
        //          args[0]         args[1]         args[2]

        switch (commandKey) {
            // handles leaving the group function
            case "!quit":
                consoleDisplay("Disconnecting...");
                try { closeConnections(socket,out,in); } catch (Exception ignored) {}
                return; // kill the Client program

            // gets all the available commands
            case "!help":
                consoleDisplay("Available commands:");
                consoleDisplay("!quit                       - leave Server");
                consoleDisplay("!help                       - show commands");
                consoleDisplay("!status                     - show everyone's status");
                consoleDisplay("!dm <friendName> <message>  - Send a private message to a friend");
                consoleDisplay("!status <text>              - set your status");
                consoleDisplay("!friend <username>          - send a friend request to a user");
                consoleDisplay("!friends                    - list your friends");
                consoleDisplay("!create <groupName>         - create a group chat");
                consoleDisplay("!join <groupName>           - join a group chat");
                consoleDisplay("!sendfile <username>        - send a file to a user");
                consoleDisplay("!stream <username>          - stream a video to a user");
                consoleDisplay("!leave                      - leave the current group chat");
                consoleDisplay("!gm <groupName> <message>   - send message to group chat");
                break;

            // gets the status of all users in the Server and can also change ur own status
            case "!status":
                if (args.length == 1) {
                    consoleDisplay(statusService.listStatuses()); // display everyones statuses to THIS user
                } else { // if there is something after !status then it should set their status as that
                    String newStatus = command.substring("!status".length()).trim();

                    statusService.setStatus(username, newStatus);

                    broadcastMessage("SERVER: " + username + " is now [" + newStatus + "]");
                    consoleDisplay("Your status is now [" + newStatus + "]");
                }
                break;

            case "!friend":
                if (args.length == 2) {
                    String targetUser = args[1];
                    String resultMessage = friendService.addFriend(username, targetUser);
                    consoleDisplay(resultMessage);

                } else {
                    consoleDisplay("Usage: !friend <username>");
                }
                break;

            case "!friends":
                consoleDisplay("Your friends: " + friendService.getFriends(username));
                break;

            // !dm user message
            case "!dm":
                if (args.length != 3) {
                    consoleDisplay("Usage: !dm <friendName> <message>");
                    return;
                }

                handelDM(args[1], args[2]);
                break;

            // create sa new group and automatically adds the user to it
            case "!create":
                if (args.length < 2) {
                    consoleDisplay("Usage: !create <groupName>");
                    return;
                }
                handleCreate(args[1]);
                break;

            // joins the group
            case "!join":
                if (args.length < 2) {
                    consoleDisplay("Usage: !join <groupName>");
                    return;
                }
                handleJoin(args[1]);
                break;

            case "!leave":
                handleLeaveGroup();
                break;

            // sends a msg to a group without even joining (this is a bomb for sure)
            case "!gm":
                if (args.length < 3) {
                    consoleDisplay("Usage: !gm <groupName> <message>");
                    return;
                }

                handleGroupMessage(args[1], args[2]);
                break;

            // sends a file such as (pdf, txt, mp4, png, jpg)
            case "!sendfile":
                if (args.length < 2) {
                    consoleDisplay("Usage: !sendfile <username>");
                    return;
                }
                handleSendFile(args[1]);
                break;

            // sends a file such as (pdf, txt, mp4, png, jpg)
            case "!stream":
                if (args.length < 2) {
                    consoleDisplay("Usage: !stream <username>");
                    return;
                }
                handleStream(args[1]);
                break;

            // if the commands start from '!' but didnt match the above options
            default:
                consoleDisplay("Unknown command. Type !help");
        }
    }

    /**
     * Displays Server messages in users console
     *
     * @param message the message to show
     */
    private void consoleDisplay(String message) {
        try {
            out.write("SERVER: " + message);
            out.newLine();
            out.flush();
        } catch (IOException e) {
            System.out.println("Error displaying in console: " + e.getMessage());
        }
    }

    /**
     * Removes the client from the list
     */
    public void removeClient() {
        if (currentGroup != null) redisManager.leaveGroup(currentGroup, username, Server.getServerID());

        redisManager.removeUserServer(username);
        clients.remove(this); // removes the current client
        statusService.userLeft(username); // not sure if i should remove user or set them to offline?
        friendService.removeUser(username);
        broadcastMessage("SERVER: " + username + " has left the chat.");
    }

    /**
     * closses all the connection if the client exits the chat
     *
     * @param socket the socket connected to the client
     * @param out    the output stream to the client
     * @param in1    the input stream from the client
     */
    public void closeConnections(Socket socket, BufferedWriter out, BufferedReader in1) {
        removeClient();
        try {
            if (socket != null) socket.close();
            if (out != null) out.close();
            if (in1 != null) in1.close();
        } catch (IOException e) {
            System.out.println("Error closing connections: " + e.getMessage());
        }
    }

    // easy access to Server if its Shutting Down
    public static void closeDB(){
        usersDB.closeDB_Connection();
    }
}

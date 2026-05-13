import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class Server {

    private static int PORT;

    // Pool count for thread management
    private static final int THREAD_POOL_COUNT  = 200;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_COUNT);
    private static final String SERVER_ID = UUID.randomUUID().toString();

    private static final KafkaConsumer<String, String> consumer;
    static {
        try {
            Properties props = new Properties();
            InputStream propertiesStream = Server.class.getClassLoader().getResourceAsStream("consumer.properties");
            if (propertiesStream == null) throw new RuntimeException("consumer.properties not found");

            props.load(propertiesStream);
            props.put("group.id", "server-group-" + SERVER_ID); // Unique group ID for each server instance
            consumer = new KafkaConsumer<>(props);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Kafka configuration: " + e.getMessage());
        }
    }

    /**
     * This gets the Servers IP address and prints it to the console so that the user can connect to it
     * @return an IP address as a String, if it fails to get the IP address it will return "localhost"
     */
    private static String getIp(){
        try{
            return InetAddress.getLocalHost().getHostAddress();
        } catch(Exception e){
            System.out.println("Server error: " + e.getMessage());
            return "localhost";
        }
    }

    /**
     * This is where the Server actually starts
     * it establishes a ServerSocket on the specified PORT
     * and waits for client connections.
     *
     * @throws IOException
     */
    public void startServer() throws IOException {
        try {

            // block for detecting ctrl+c or automatic user shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    threadPool.shutdown(); // no new task to be done
                    threadPool.awaitTermination(45, TimeUnit.SECONDS);
                    ClientHandler.closeDB(); // close the DB connection when the server is shutting down
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // restore the interrupted status
                }
            }));

            startKafkaConsumer();

            ServerSocket serverSocket = new ServerSocket(PORT);
            System.out.println("Server Running on port: " + PORT);

            System.out.println("--------------------------------------------------");
            System.out.println("Please connect to this IP: " + getIp());
            System.out.println("Current Server ID: " + SERVER_ID);
            System.out.println("--------------------------------------------------");

            while (true) {
                Socket clientSocket = serverSocket.accept(); // wait for the client to connect
                System.out.println("Client connected: " + clientSocket.getInetAddress().getHostAddress());

                ClientHandler handler = new ClientHandler(clientSocket);
                threadPool.execute(handler);

            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        }
    }

    private void startKafkaConsumer() {
        new Thread(() -> {
            consumer.subscribe(Arrays.asList("messages","direct_message")); // subscribe to both topics

            while (true) {
                try{
                    consumer.poll(Duration.ofSeconds(10)).forEach(record -> {
                        // receives -> ServerId#usernam:groupName:Message (if group messaging)
                        //          -> userA:userB:Message (if private messaging)

                        String topic = record.topic(); // determine which one is DM and GRP mesaging
                        String value = record.value();

                        if (topic.equals("direct_message")) {
                            System.out.println("Received direct message from Kafka: " + value); // DEBUG
                            
                            // Check if this DM is targeted at this server
                            String[] parts = value.split("#", 2);
                            
                            if (parts.length < 2) {
                                System.err.println("Malformed Data for Kafka Consumer");
                                return;
                            }

                            // Targeted DM format: targetServerID#sender:receiver:message
                            String targetServerID = parts[0];
                            String messageChunk = parts[1];

                            if (targetServerID.equals(SERVER_ID))
                                ClientHandler.deliverLKafkaMessageDM(messageChunk);

                        }

                        else if (topic.equals("messages")) {
                            System.out.println("Received group message from Kafka: " + record.value()); // DEBUG
                            String[] values =  value.split("#", 2);
                            if (values.length < 2) {
                                System.err.println("Malformed Data fro Kafka Consumer");
                                return;
                            }

                            String sender = values[0];
                            String messageChunk = values[1];

                            // if the server ID is same dont use Kafka
                            // else there will be 2x the same message delivery
                            if (!sender.equals(SERVER_ID))  ClientHandler.deliverKafkaMessageGroup(messageChunk);
                            System.out.println("Received message from Kafka: " + Arrays.toString(values));
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Kafka consumer error: " + e.getMessage());
                }
            }
        }).start();
    }

    // getter for server ID
    public static String getServerID(){
        return SERVER_ID;
    }

    /**
     * Ik its a lot to take but look at the comments to understand what is happening
     * This is a simple server that listens for client connections and echoes back messages
     * sent by the client. It uses TCP sockets for communication.
     */
    public static void main(String[] args) {
        System.out.println("Starting the Server...");

        PORT = (args.length > 0) ? Integer.parseInt(args[0]) : 8080;
        try {
            Server server = new Server();
            server.startServer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

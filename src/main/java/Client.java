import stage4.FileReceive;
import stage4.FileTransfer;
import stage4.StreamReceive;

import java.io.*;
import java.net.Socket;
import java.sql.SQLException;
import java.util.Scanner;

public class Client {

    private static int SERVER_PORT;

    private Socket clientSocket;
    private BufferedWriter out;
    private BufferedReader in;
    private String username;
    private String password;
    private String email;

    /**
     * Initializes the client by setting up the socket connection and input/output streams.
     * Login
     *
     * @param clientSocket Socket object
     * @param username Clients Username
     * @param password Clients Password
     */
    public Client(Socket clientSocket, String username, String password) {
        try{
            this.clientSocket = clientSocket;
            this.username = username;
            this.password = password;

            out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF-8"));
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));

            out.write("1\n");
            out.write(username + "\n");
            out.write(password + "\n");
            out.flush();

            System.out.println(username + " connected to server at " + clientSocket.getInetAddress().getHostAddress() + ":" + SERVER_PORT);
        } catch(IOException e) {
            closeConnections(clientSocket, out, in);
            System.out.println("Error initializing client: " + e.getMessage());
        }
    }

    /**
     * Initializes the client by setting up the socket connection and input/output streams.
     * Registreation
     *
     * @param clientSocket Socket object
     * @param username Clients Username
     * @param password Clients Password
     * @param email Clients Email
     */
    public Client(Socket clientSocket, String username, String password, String email) {
        try{
            this.clientSocket = clientSocket;
            this.username = username;
            this.password = password;
            this.email = email;

            out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF-8"));
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));

            out.write("2\n");
            out.write(username + "\n");
            out.write(password + "\n");
            out.write(email + "\n");
            out.flush();

            System.out.println(username + " connected to server at " + clientSocket.getInetAddress().getHostAddress() + ":" + SERVER_PORT);
        } catch(IOException e) {
            closeConnections(clientSocket, out, in);
            System.out.println("Error initializing client: " + e.getMessage());
        }
    }

    /**
     * first sends the username to the server,
     * then continuously reads user input from the console and sends it to the server.
     */
    public void sendMessage() {
        try {
            String serverMsg = in.readLine();
            if (serverMsg.equals("AUTH_FAILURE")) {
                System.out.println("Authentication failed. Please check your credentials and try again.");
                return;
            }

            if (serverMsg.equals("REG_FAILURE")) {
                System.out.println("Registration failed. Please check your credentials and try again.");
                return;
            }

            Scanner scanner = new Scanner(System.in);

            // print commands
            System.out.println("--- Available Commands ---");
            System.out.println("!help - get all commands ");
            System.out.println("exit  - disconnecting");
            System.out.println("-------------------------- ");

            while(!clientSocket.isClosed()) {
                String message = scanner.nextLine();

                if (message.trim().equals("")) {
                    continue;
                }

                out.write(message + "\n");
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("Error sending message: " + e.getMessage());
        }
    }

    /**
     * Constantly listens for messages from Server
     */
    public void listenMessages() {
        new Thread(() -> {
            String messageFromServer;

            while (!clientSocket.isClosed()) {
                try {
                    messageFromServer = in.readLine();

                    if (messageFromServer == null) {
                        System.out.println("Server has closed the connection.");
                        closeConnections(clientSocket, out, in);
                        break;
                    }
                    if (messageFromServer.startsWith("FILE_RECEIVER_READY:")){
                        // this is the format FILE_RECEIVER_READY:port:clientA/B
                        String[] messageParts = messageFromServer.split(":");
                        int port = Integer.parseInt(messageParts[1]);
                        System.out.println(messageFromServer);
                        FileReceive.startReceiver(port);
                    }
                    if  (messageFromServer.startsWith("FILE_SENDER_READY:")){
                        // format is FILE_SENDER_READY:receiversIP:port
                        String[] messageParts = messageFromServer.split(":");
                        String receiverIP  = messageParts[1];
                        int port = Integer.parseInt(messageParts[2]);
                        System.out.println(messageFromServer);
                        FileTransfer.openSenderGUI(receiverIP, port);
                    }

                    if (messageFromServer.startsWith("STREAM_RECEIVER_READY:")) {
                        String[] parts = messageFromServer.split(":");
                        int port = Integer.parseInt(parts[1]);
                        System.out.println(messageFromServer);

                        new Thread(() -> new StreamReceive(port).receiveAndPlay()).start();
                        continue;
                    }

                    if (messageFromServer.startsWith("STREAM_SENDER_READY:")) {
                        String[] parts = messageFromServer.split(":");
                        String ip = parts[1];
                        int port = Integer.parseInt(parts[2]);
                        System.out.println(messageFromServer);

                        FileTransfer.openStreamGUI(ip, port);
                        continue;
                    }

                    else {
                        System.out.println(messageFromServer);
                    }

                } catch (IOException e) {
                    closeConnections(clientSocket, out, in);
                    break;
                }
            }
        }).start();
    }

    /**
     * Classes all connection for a client
     *
     * @param clientSocket Socket that needs to be removed
     * @param out Writer stream that needs to be removed
     * @param in Reader stream that needs to be removed
     */
    public void closeConnections(Socket clientSocket, BufferedWriter out, BufferedReader in){
        try{
            if(!clientSocket.isClosed()) clientSocket.close();
            if(out != null) out.close();
            if(in != null) in.close();
        } catch (Exception e){
            System.out.println("Error closing connections: " + e.getMessage());
        }
    }


    public static void main(String[] args) throws IOException, SQLException {
        System.out.print("Enter your username: ");
        Scanner scanner = new Scanner(System.in);

        //System.out.println("Connecting to server at " + SERVER_IP + ":" + SERVER_PORT + "...");
        System.out.print("Enter the Server's IP address (e.g., 192.168.1.5): or connect to the localhost\n");
        String serverIp = scanner.nextLine();

        System.out.print("Enter the Server's port: ");
        SERVER_PORT = scanner.nextInt();
        scanner.nextLine(); // consume the newline character

        System.out.println("Connecting to server at " + serverIp + ":" + SERVER_PORT + "...");
        Socket socket = new Socket(serverIp, SERVER_PORT);

        System.out.println("Choosse the following options: \n 1. Login \n 2. Register");
        int option = scanner.nextInt();
        scanner.nextLine(); // consume the newline character

        Client client = null;
        if (option == 1) {
            System.out.println("Enter your username: ");
            String username = scanner.nextLine();
            System.out.println("Enter your password: ");
            String password = scanner.nextLine();

            client = new Client(socket, username, password);
        } else if (option == 2) {
            System.out.println("Enter your username: ");
            String username = scanner.nextLine();
            System.out.println("Enter your password: ");
            String password = scanner.nextLine();
            System.out.println("Enter your email: ");
            String email = scanner.nextLine();

            client = new Client(socket, username, password, email);
        }


        client.listenMessages(); // start listening for messages from server
        client.sendMessage(); // start sending messages to server
    }
}

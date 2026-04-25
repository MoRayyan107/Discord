package stage1;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class P2P {

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private Scanner inputScanner;

    // Host the connection
    public P2P(int port) throws IOException {
        System.out.println("Establishing connection on port: " + port);
        ServerSocket clientServerSocket = new ServerSocket(port);

        System.out.println("Waiting for a Friend to connect...");
        this.socket = clientServerSocket.accept();

        System.out.println("Connection established with: " + socket.getInetAddress().getHostAddress());
        clientServerSocket.close();

        // Initialize reader and writer for communication
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
    }

    // this is where the peers start connecting
    public P2P(String host, int port) throws IOException {
        System.out.println("Connecting to " + host + " on port " + port);
        this.socket = new Socket(host, port);
        System.out.println("Connected to: " + socket.getInetAddress().getHostAddress());

        // Initialize reader and writer for communication
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
    }

    public void startChat(){
        System.out.println("You can start chatting now! Type 'exit' to quit.");
        inputScanner = new Scanner(System.in);

        Thread recieveChats = new Thread(() -> {
            String msgFromPeer;

            try {
                while (true) {
                    msgFromPeer = reader.readLine();
                    if (msgFromPeer == null) {
                        System.out.println("Peer disconnected.");
                        break;
                    }
                    System.out.println("Peer: " + msgFromPeer);

                }
            } catch (IOException e) {
                System.out.println("Connection closed.");
            }
        });
        recieveChats.start();

        // this si where the user sends messages to the peer
        try {
            while (true) {
                String msgToPeer = inputScanner.nextLine();
                writer.write(msgToPeer + "\n");
                writer.flush();

                if (msgToPeer.equalsIgnoreCase("exit")) {
                    System.out.println("Exiting chat...");
                    socket.close();
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("Connection closed.");
        } finally {
            closeConnections(socket, reader, writer, inputScanner);
        }
    }

    private void closeConnections(Socket socket, BufferedReader reader, BufferedWriter writer, Scanner inputScanner) {
        try{
            if (reader != null) reader.close();
            if (writer != null) writer.close();
            if (socket != null) socket.close();
            if (inputScanner != null) inputScanner.close();
        } catch (IOException e){
            System.out.println("Error closing connections: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Stage 1: P2P Chatter");
        System.out.println("Choose a mode: \n(1) Host \n(2) Connect to peer");
        Scanner modeScanner = new Scanner(System.in);
        String mode = modeScanner.nextLine();

        try {
            P2P chats = null;
            if (mode.equals("1")) {
                System.out.println("Enter port to host on: ex 8888");
                int port = modeScanner.nextInt();
                modeScanner.nextLine();
                chats = new P2P(port);

            } else if (mode.equals("2")) {
                System.out.println("Enter peer IP address:");
                String host = modeScanner.nextLine();
                System.out.println("Enter peer port:");
                int port = modeScanner.nextInt();
                modeScanner.nextLine(); // consume the newline so no issues arise
                chats = new P2P(host, port);
            } else {
                System.out.println("Invalid mode selected. Exiting.");
            }

            if (chats != null) {
                chats.startChat();
            }
            modeScanner.close();

        } catch (Exception e){
            System.out.println("An error occurred: " + e.getMessage());
        }
    }
}

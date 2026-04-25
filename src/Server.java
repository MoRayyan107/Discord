import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {

    private static final int PORT = 8888;

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
            ServerSocket serverSocket = new ServerSocket(PORT);
            System.out.println("Server Running on port: " + PORT);

            System.out.println("--------------------------------------------------");
            System.out.println("Please connect to this IP: " + getIp());
            System.out.println("--------------------------------------------------");

            while (true) {
                Socket clientSocket = serverSocket.accept(); // wait for the client to connect
                System.out.println("Client connected: " + clientSocket.getInetAddress().getHostAddress());

                ClientHandler handler = new ClientHandler(clientSocket);
                Thread thread = new Thread(handler);

                thread.start();
            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        }
    }

    /**
     * Ik its a lot to take but look at the comments to understand what is happening
     * This is a simple server that listens for client connections and echoes back messages
     * sent by the client. It uses TCP sockets for communication.
     */
    public static void main(String[] args) {
        System.out.println("Starting the Server...");

        try {
            Server server = new Server();
            server.startServer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

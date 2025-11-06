import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class MulticastChatClient {

    private static final String GROUP_ADDRESS = "230.0.0.1";
    private static final int MULTICAST_PORT = 5000;
    private static final int SERVER_LISTEN_PORT = 6000;

    public static void main(String[] args) throws Exception {
        MulticastSocket multicastSocket = new MulticastSocket(MULTICAST_PORT);
        InetAddress group = InetAddress.getByName(GROUP_ADDRESS);
        multicastSocket.joinGroup(group);

        DatagramSocket sendSocket = new DatagramSocket();

        String localIP = InetAddress.getLocalHost().getHostAddress();
        System.out.println("=== IFSE Multicast Chat Client ===");
        System.out.println("Cliente IP: " + localIP);

        // Registro automático
        String registerMsg = "REGISTER " + localIP;
        sendSocket.send(new DatagramPacket(registerMsg.getBytes(), registerMsg.length(),
                InetAddress.getByName("10.0.0.1"), SERVER_LISTEN_PORT));

        // Hilo receptor
        new Thread(() -> {
            byte[] buffer = new byte[1024];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    multicastSocket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    System.out.println("\n📡 " + message);
                    System.out.print("Responder > ");
                } catch (IOException e) {
                    System.out.println("Error recibiendo: " + e.getMessage());
                }
            }
        }).start();

        // Bucle de respuestas
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print("Responder > ");
            String response = sc.nextLine();
            if (response.equalsIgnoreCase("exit")) break;

            String fullMessage = "[Cliente " + localIP + " " + timestamp() + "] " + response;
            sendSocket.send(new DatagramPacket(fullMessage.getBytes(), fullMessage.length(),
                    InetAddress.getByName("10.0.0.1"), SERVER_LISTEN_PORT));
        }

        multicastSocket.leaveGroup(group);
        multicastSocket.close();
        sendSocket.close();
    }

    private static String timestamp() {
        return new SimpleDateFormat("HH:mm:ss").format(new Date());
    }
}

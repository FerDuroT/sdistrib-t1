import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class MulticastChatServer {

    private static final String GROUP_ADDRESS = "230.0.0.1";
    private static final int MULTICAST_PORT = 5000;
    private static final int SERVER_LISTEN_PORT = 6000;

    private static ConcurrentHashMap<String, Long> activeClients = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        InetAddress group = InetAddress.getByName(GROUP_ADDRESS);
        DatagramSocket multicastSocket = new DatagramSocket();
        DatagramSocket responseSocket = new DatagramSocket(SERVER_LISTEN_PORT);

        System.out.println("=== IFSE Multicast Chat Server v2 ===");
        System.out.println("Grupo: " + GROUP_ADDRESS + " | Puerto: " + MULTICAST_PORT);
        System.out.println("-------------------------------------");

        // Hilo para escuchar respuestas y registros
        executor.submit(() -> {
            byte[] buffer = new byte[1024];
            while (true) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    responseSocket.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength());
                    String clientIP = packet.getAddress().getHostAddress();

                    activeClients.put(clientIP, System.currentTimeMillis());

                    if (msg.startsWith("REGISTER")) {
                        log("Nuevo cliente registrado: " + clientIP);
                    } else {
                        log("Respuesta desde " + clientIP + ": " + msg);
                    }

                } catch (IOException e) {
                    log("Error escuchando respuestas: " + e.getMessage());
                }
            }
        });

        // Hilo para verificar actividad de clientes
        executor.submit(() -> {
            while (true) {
                long now = System.currentTimeMillis();
                activeClients.forEach((ip, lastSeen) -> {
                    if (now - lastSeen > 30000) { // 30 seg
                        log("Cliente inactivo: " + ip);
                        activeClients.remove(ip);
                    }
                });
                try { Thread.sleep(10000); } catch (InterruptedException e) {}
            }
        });

        // Envío de mensajes
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.print("Mensaje (exit/list): ");
            String message = sc.nextLine();

            if (message.equalsIgnoreCase("exit")) break;

            if (message.equalsIgnoreCase("list")) {
                System.out.println("Clientes activos:");
                activeClients.forEach((ip, lastSeen) -> 
                    System.out.println(" - " + ip + " (último: " + 
                        new SimpleDateFormat("HH:mm:ss").format(new Date(lastSeen)) + ")"));
                continue;
            }

            String fullMessage = "[Servidor " + timestamp() + "] " + message;
            byte[] data = fullMessage.getBytes();
            multicastSocket.send(new DatagramPacket(data, data.length, group, MULTICAST_PORT));
            log("Mensaje enviado: " + fullMessage);
        }

        multicastSocket.close();
        responseSocket.close();
        executor.shutdown();
    }

    private static void log(String msg) {
        System.out.println("[LOG " + timestamp() + "] " + msg);
    }

    private static String timestamp() {
        return new SimpleDateFormat("HH:mm:ss").format(new Date());
    }
}

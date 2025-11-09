#!/usr/bin/env python3                       # Shebang: permite ejecutar el archivo como script con Python 3
"""
Multicast + Concurrency demo (UDP) for distributed systems labs.
- Runs as a simple multicast chat/heartbeat node.
- Demonstrates: joining/leaving a multicast group, sending messages, receiving concurrently,
  and processing messages in a worker pool.
Tested on Python 3.10+ (Linux, macOS, Windows).
"""
import argparse                              # Para leer parámetros/flags desde la línea de comandos
import socket                                # API de sockets (UDP, multicast)
import struct                                # Empaquetar datos binarios (mreq para unirse al grupo)
import sys                                   # Acceso a stdin/stdout/exit
import threading                             # Hilos (concurrencia)
import queue                                 # Cola segura entre hilos
import time                                  # Dormir/pausas (heartbeat)
from datetime import datetime                # Timestamps legibles

# ---------- Worker que procesa mensajes entrantes de forma concurrente ----------
def worker_loop(in_q: "queue.Queue[tuple[str, bytes]]",  # Cola de (addr, data) compartida
                node_name: str,                          # Nombre del nodo (solo para imprimir)
                stop_event: threading.Event):            # Evento para detener hilos limpiamente
    while not stop_event.is_set():                       # Bucle hasta que pidan detener
        try:
            addr, data = in_q.get(timeout=0.25)          # Toma un elemento de la cola (con timeout)
        except queue.Empty:                              # Si no hay, vuelve a intentar
            continue
        try:
            msg = data.decode('utf-8', errors='replace') # Decodifica bytes a texto (UTF-8)
        except Exception:
            msg = repr(data)                             # Si falla, muestra representación cruda
        stamp = datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]  # Timestamp HH:MM:SS.mmm en UTC
        print(f"[{stamp}] [proc@{node_name}] from {addr}: {msg}")  # Log del procesamiento
        in_q.task_done()                                 # Marca el trabajo como completado en la cola


def main():
    parser = argparse.ArgumentParser(                    # Define CLI: descripción del programa
        description="Multicast + Concurrency Demo (chat & heartbeat).")
    parser.add_argument("--group", default="239.255.0.1",# Grupo multicast IPv4 por defecto
                        help="Multicast group (IPv4). Default: 239.255.0.1")
    parser.add_argument("--port", type=int, default=50000,# Puerto UDP por defecto
                        help="UDP port. Default: 50000")
    parser.add_argument("--iface", default="0.0.0.0",    # IP local de la interfaz para unirse/enviar
                        help="Local interface IP used for joining/sending (e.g., your ZeroTier/Hamachi IP).")
    parser.add_argument("--name", default=None,          # Alias del nodo (si no, hostname)
                        help="Node name (defaults to hostname).")
    parser.add_argument("--ttl", type=int, default=1,    # TTL multicast (alcance de saltos)
                        help="Multicast TTL (hops). Default: 1")
    parser.add_argument("--workers", type=int, default=2,# Nº de hilos trabajadores
                        help="Number of concurrent processing workers. Default: 2")
    parser.add_argument("--no_heartbeat", action="store_true",  # Flag para desactivar heartbeat
                        help="Disable automatic heartbeat messages.")
    args = parser.parse_args()                           # Parsea argumentos

    node_name = args.name or socket.gethostname()        # Si no dieron --name, usa hostname

    group = socket.inet_aton(args.group)                 # Convierte IP de grupo a binario (4 bytes)
    iface_ip = socket.inet_aton(args.iface)              # Convierte IP de interfaz a binario

    # --- Crear socket UDP ---
    sock = socket.socket(socket.AF_INET,                 # Familia IPv4
                         socket.SOCK_DGRAM,              # Tipo datagrama (UDP)
                         socket.IPPROTO_UDP)             # Protocolo UDP
    # Permite reusar dirección/puerto (varios procesos/labs en el mismo host)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    except Exception:
        pass

    # Enlaza el socket al puerto indicado en todas las interfaces locales (para recibir)
    try:
        sock.bind(("", args.port))                       # "" = 0.0.0.0 (todas las interfaces)
    except OSError as e:
        print(f"Bind failed on port {args.port}: {e}", file=sys.stderr)  # Error de bind
        sys.exit(2)                                      # Salida con código de error

    # Unirse al grupo multicast en la interfaz especificada
    mreq = struct.pack("=4s4s", group, iface_ip)         # Estructura mreq: (grupo, interfaz)
    sock.setsockopt(socket.IPPROTO_IP,                   # Opción de IP a nivel del protocolo
                     socket.IP_ADD_MEMBERSHIP,           # Unirse al grupo
                     mreq)

    # Asegura que los envíos salgan por la interfaz indicada (no por Wi-Fi/ethernet)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, iface_ip)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, args.ttl)  # TTL de los datagramas

    # Cola de entrada y creación de workers concurrentes
    in_q: "queue.Queue[tuple[str, bytes]]" = queue.Queue(maxsize=1024)  # Búfer de mensajes
    stop_event = threading.Event()                       # Señal de parada compartida
    workers = []                                         # Lista de hilos worker
    for i in range(max(1, args.workers)):                # Crea al menos 1 worker
        t = threading.Thread(target=worker_loop,         # Hilo que ejecuta worker_loop(...)
                             args=(in_q, node_name, stop_event),
                             daemon=True)                # Daemon: se cierra con el proceso
        t.start()
        workers.append(t)

    # Hilo receptor: mete datagramas en la cola para que los workers los procesen
    def recv_loop():
        while not stop_event.is_set():                   # Recibe hasta que pidan parar
            try:
                data, addr = sock.recvfrom(65535)        # Recibe un datagrama (máx ~64 KB)
            except OSError:                              # Socket cerrado u otro error: salir
                break
            try:
                in_q.put_nowait((f"{addr[0]}:{addr[1]}", data))  # Encola (IP:puerto, datos)
            except queue.Full:
                # Si la cola está llena, se descarta (simula backpressure/perdida controlada)
                pass

    recv_t = threading.Thread(target=recv_loop, daemon=True)  # Crea hilo receptor
    recv_t.start()                                            # Inicia hilo receptor

    """
    # Hilo opcional de heartbeat: envía “HB ...” cada 5 s al grupo
    def heartbeat_loop():
        while not stop_event.is_set():
            hb = f"HB from {node_name} @ {datetime.utcnow().isoformat()}Z"  # Mensaje HB con timestamp
            try:
                sock.sendto(hb.encode("utf-8"),               # Envía datagrama UDP
                           (socket.inet_ntoa(group), args.port))
            except Exception as e:
                print(f"Heartbeat send failed: {e}", file=sys.stderr)  # Log de error al enviar
            time.sleep(5)                                     # Espera 5 s

    if not args.no_heartbeat:                                 # Si no desactivaron heartbeat...
        hb_t = threading.Thread(target=heartbeat_loop, daemon=True)  # Crea hilo HB
        hb_t.start()                                          # Inicia hilo HB
    else:
        hb_t = None                                           # No habrá hilo HB
    """

    # Mensaje de bienvenida con configuración efectiva
    print("----------------------------------------------------------")
    print(f"Multicast node '{node_name}' on group {socket.inet_ntoa(group)}:{args.port}")
    print(f"Interface: {args.iface} | Workers: {len(workers)} | TTL: {args.ttl}")
    print("Type a message and press ENTER to multicast. Ctrl+C to exit.")
    print("----------------------------------------------------------")

    try:
        while True:                                          # Bucle principal de envío manual
            line = sys.stdin.readline()                      # Lee una línea desde la consola
            if not line:                                     # EOF (p.ej., cierre de stdin)
                break
            line = line.rstrip("\n")                         # Quita salto de línea
            if not line:                                     # Si está vacía, ignora
                continue
            payload = f"[{node_name}] {line}".encode('utf-8')# Prepara mensaje con etiqueta del nodo
            sock.sendto(payload,                             # Envía al grupo/puerto
                        (socket.inet_ntoa(group), args.port))
    except KeyboardInterrupt:                                # Ctrl+C: salir limpiamente
        pass
    finally:
        stop_event.set()                                     # Señal para detener todos los hilos
        try:
            # Abandona la membresía del grupo (mejor esfuerzo)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
        except Exception:
            pass
        sock.close()                                         # Cierra el socket
        recv_t.join(timeout=1.0)                             # Espera fin del receptor (hasta 1s)
        for t in workers:                                    # Espera fin de los workers
            t.join(timeout=1.0)
        if hb_t:                                             # Si existía hilo heartbeat, espera su fin
            hb_t.join(timeout=1.0)


if __name__ == "__main__":                                  # Punto de entrada del script
    main()                                                  # Llama a main()

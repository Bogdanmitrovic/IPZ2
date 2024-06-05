import socket
import time
import threading


def handle_client(client_socket, data_file_path):
    with open(data_file_path, mode="r", encoding='utf-8-sig') as data_file:
        lines = data_file.readlines()
        timestamp = ''
        i = 1
        count = 0
        try:
            while i < len(lines):
                if timestamp == '':
                    timestamp = lines[i].split(",")[0]
                elif timestamp != lines[i].split(",")[0]:
                    time.sleep(3)
                    timestamp = lines[i].split(",")[0]
                    count = 0
                if count < 20:
                    client_socket.send(str(lines[i]).encode())
                i += 1
                count += 1
        except KeyboardInterrupt:
            print("Uspesno izvrseno!")
        finally:
            client_socket.close()


def start_server(host, port, data_file_path):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Server listening on {host}:{port}")

    while True:
        client_socket, address = server_socket.accept()
        print(f"Client connected from {address[0]}:{address[1]}")
        client_thread = threading.Thread(target=handle_client, args=(client_socket, data_file_path))
        client_thread.start()


if __name__ == "__main__":
    start_server('localhost', 9999, "plant_generation_data.csv")

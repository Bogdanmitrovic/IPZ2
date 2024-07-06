import socket
import time
import threading


def handle_client(client_socket, data_file_path):
    with (open(data_file_path + '1.csv', mode="r", encoding='utf-8-sig') as data_file1,
          open(data_file_path + '2.csv', mode="r", encoding='utf-8-sig') as data_file2):
        lines1 = data_file1.readlines()
        lines2 = data_file2.readlines()
        lines = [lines1, lines2]
        timestamp = ''
        indices = [1, 1]
        flag = 0
        count = 0
        try:
            while indices[flag] < len(lines[flag]):
                if timestamp == '':
                    timestamp = lines[flag][indices[flag]].split(",")[0]
                elif timestamp != lines[flag][indices[flag]].split(",")[0]:
                    flag += 1
                    if flag == 2:
                        flag = 0
                        time.sleep(3)
                    timestamp = lines[flag][indices[flag]].split(",")[0]
                    count = 0
                send_data = str(flag + 1) + ',' + str(lines[flag][indices[flag]])
                client_socket.send(send_data.encode())
                indices[flag] += 1
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
    start_server('localhost', 9999, "plant_generation_data")

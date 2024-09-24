import grpc
from concurrent import futures
import reservation_pb2
import reservation_pb2_grpc
import threading
import time

def read_config(config_name):
    # чтение конфигурационного файла и возвращение словаря с настройками
    config = {}
    with open(config_name, 'r') as file:
        for line in file:
            key, value = map(str.strip, line.split('='))
            config[key] = value
    return config

class ReservationServerServicer(reservation_pb2_grpc.ReservationServiceServicer):
    def __init__(self):
        # инициализация ReservationServerServicer с каналами и блокировками
        self.seat_management_stubs = []
        self.seat_management_channels = []
        self.seat_management_stub_lock = threading.Lock()
        self.current_seat_management_server = 0
        self.server_statuses = []

        # Создаем каналы здесь
        for i in range(int(config['SeatManagementServerCount'])):
            self.add_server_channel(
                i + 1,
                f"SeatManagementServerIP{i + 1}",
                f"SeatManagementServerPort{i + 1}",
                self.seat_management_channels,
                self.seat_management_stubs,
                self.server_statuses
            )

    def add_server_channel(self, server_number, ip_key, port_key, channels, stubs, statuses):
        server_ip = config[ip_key]
        server_port = int(config[port_key])
        channel = grpc.insecure_channel(f"{server_ip}:{server_port}")
        stub = reservation_pb2_grpc.ReservationServiceStub(channel)
        channels.append(channel)
        stubs.append(stub)
        statuses.append(True)  # Изначально считаем сервера неактивными

    def get_next_active_seat_management_server(self):
        # получение следующего активного сервера управления местами
        with self.seat_management_stub_lock:
            active_seat_management_servers = [i for i, status in
                                              enumerate(self.server_statuses)
                                              if status]

            if not active_seat_management_servers:
                return None

            self.current_seat_management_server = (self.current_seat_management_server + 1) % len(active_seat_management_servers)
            return active_seat_management_servers[self.current_seat_management_server]

    def check_server_availability(self, server_number, channels, statuses):
        while True:
            # периодическая проверка доступности сервера управления местами
            time.sleep(1)
            with self.seat_management_stub_lock:
                channel = channels[server_number - 1]
                try:
                    grpc.channel_ready_future(channel).result(timeout=1)
                    statuses[server_number - 1] = True
                except grpc.FutureTimeoutError:
                    statuses[server_number - 1] = False

    def start_server_availability_check(self, server_number, channels, statuses):
        # запуск потока для проверки доступности сервера управления местами
        thread = threading.Thread(
            target=self.check_server_availability,
            args=(server_number, channels, statuses),
            daemon=True
        )
        thread.start()

    def ReserveSeat(self, request, context):
        # бронирование мест
        active_seat_management_server = self.get_next_active_seat_management_server()
        if active_seat_management_server is None:
            return reservation_pb2.ReservationResponse(
                success=False,
                message="Извините, все сервера управления местами недоступны. Попробуйте позже"
            )

        stub = self.seat_management_stubs[active_seat_management_server]
        seat_status_response = stub.ReserveSeat(request)
        seat_status_response.message+=f" (север бронирования 1)"
        return seat_status_response

def serve_reservation_server():
    global config
    config = read_config("confreserv.txt")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reservation_servicer = ReservationServerServicer()
    reservation_pb2_grpc.add_ReservationServiceServicer_to_server(reservation_servicer, server)
    # запуск проверки доступности серверов управления местами
    for i in range(int(config['SeatManagementServerCount'])):
        reservation_servicer.start_server_availability_check(i + 1, reservation_servicer.seat_management_channels, reservation_servicer.server_statuses)
    # запуск сервера
    server.add_insecure_port(f"{config['IP']}:{config['Port']}")
    server.start()
    print(f"Сервер бронирования запущен и слушает на {config['IP']}:{config['Port']}...")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Сервер бронирования завершает работу")

if __name__ == '__main__':
    serve_reservation_server()

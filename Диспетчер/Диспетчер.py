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

class DispatcherServicer(reservation_pb2_grpc.ReservationServiceServicer):
    def __init__(self):
        self.reservation_stubs = []
        self.seat_management_stubs = []
        self.reservation_channels = []
        self.seat_management_channels = []
        self.reservation_stub_lock = threading.Lock()
        self.seat_management_stub_lock = threading.Lock()
        self.current_reservation_server = 0
        self.current_seat_management_server = 0
        self.server_statuses = []

        # создание каналов для серверов бронирования и управления местами
        for i in range(int(config['ReservationServerCount'])):
            self.add_server_channel(
                i + 1,
                f"ReservationServerIP{i + 1}",
                f"ReservationServerPort{i + 1}",
                self.reservation_channels,
                self.reservation_stubs,
                self.server_statuses
            )

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
        # добавление канала и стаба для сервера
        server_ip = config[ip_key]
        server_port = int(config[port_key])
        channel = grpc.insecure_channel(f"{server_ip}:{server_port}")
        stub = reservation_pb2_grpc.ReservationServiceStub(channel)
        channels.append(channel)
        stubs.append(stub)
        statuses.append(True)

    def get_next_active_reservation_server(self):
        # получение следующего активного сервера бронирования
        with self.reservation_stub_lock:
            active_reservation_servers = [i for i, status in
                                          enumerate(self.server_statuses[:int(config['ReservationServerCount'])])
                                          if status]

            if not active_reservation_servers:
                return None

            self.current_reservation_server = (self.current_reservation_server + 1) % len(active_reservation_servers)
            return active_reservation_servers[self.current_reservation_server]

    def get_next_active_seat_management_server(self):
        # получение следующего активного сервера управления местами
        with self.seat_management_stub_lock:
            active_seat_management_servers = [i for i, status in
                                              enumerate(self.server_statuses[int(config['ReservationServerCount']):])
                                              if status]

            if not active_seat_management_servers:
                return None

            self.current_seat_management_server = (self.current_seat_management_server + 1) % len(active_seat_management_servers)
            return active_seat_management_servers[self.current_seat_management_server]

    def check_server_availability(self, server_number, channels, statuses):
        # проверка доступности сервера в отдельном потоке
        while True:
            time.sleep(1)
            with self.reservation_stub_lock:
                channel = channels[server_number - 1]
                try:
                    grpc.channel_ready_future(channel).result(timeout=1)
                    statuses[server_number - 1] = True
                except grpc.FutureTimeoutError:
                    statuses[server_number - 1] = False

    def start_server_availability_check(self, server_number, channels, statuses):
        # запуск потока для проверки доступности сервера
        thread = threading.Thread(
            target=self.check_server_availability,
            args=(server_number, channels, statuses),
            daemon=True
        )
        thread.start()

    def Ping(self, request, context):
        # проверка доступности диспетчера
        return reservation_pb2.PingResponse()

    def ReserveSeat(self, request, context):
        active_reservation_server = self.get_next_active_reservation_server()
        if active_reservation_server is None:
            return reservation_pb2.ReservationResponse(
                success=False,
                message="Извините, все сервера бронирования недоступны. Попробуйте позже."
            )

        stub = self.reservation_stubs[active_reservation_server]
        reservation_response = stub.ReserveSeat(request)
        return reservation_response

    def GetSeatStatus(self, request, context):
        active_seat_management_server = self.get_next_active_seat_management_server()
        if active_seat_management_server is None:
            return reservation_pb2.SeatStatusResponse(
                success=False,
                message="Извините, все сервера управления местами недоступны. Попробуйте позже."
            )

        stub = self.seat_management_stubs[active_seat_management_server]
        seat_status_response = stub.GetSeatStatus(request)
        return seat_status_response

def serve_dispatcher():
    global config
    config = read_config("confdispatch.txt")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dispatcher_servicer = DispatcherServicer()
    reservation_pb2_grpc.add_ReservationServiceServicer_to_server(dispatcher_servicer, server)

    for i in range(int(config['ReservationServerCount'])):
        dispatcher_servicer.start_server_availability_check(i + 1, dispatcher_servicer.reservation_channels, dispatcher_servicer.server_statuses)

    for i in range(int(config['SeatManagementServerCount'])):
        dispatcher_servicer.start_server_availability_check(i + 1, dispatcher_servicer.seat_management_channels, dispatcher_servicer.server_statuses)

    server.add_insecure_port(f"{config['IP']}:{config['Port']}")
    # запуск диспетчера
    server.start()
    print(f"Диспетчер запущен и слушает на {config['IP']}:{config['Port']}...")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Диспетчер завершает работу")

if __name__ == '__main__':
    serve_dispatcher()
import grpc
import time
import threading
import reservation_pb2
import reservation_pb2_grpc

class Client:
    def __init__(self, config_file):
        # чтение конфигурационного файла при инициализации клиента
        self.config = self.read_config(config_file)
        # канал для соединения с диспетчером
        self.dispatcher_channel = grpc.insecure_channel(
            f"{self.config['DispatcherServerIP']}:{self.config['DispatcherServerPort']}"
        )
        # блокировки и события
        self.lock = threading.Lock()
        self.dispatcher_available = True
        self.result_received_event = threading.Event()

    def read_config(self, config_file):
        # чтение конфигурационного файла и возвращение словаря с настройками
        config = {}
        with open(config_file, 'r') as file:
            for line in file:
                key, value = map(str.strip, line.split('='))
                config[key] = value
        return config

    def check_dispatcher_availability(self):
        try:
            # проверка доступности диспетчера
            stub = reservation_pb2_grpc.ReservationServiceStub(self.dispatcher_channel)
            ping_request = reservation_pb2.PingRequest()
            response = stub.Ping(ping_request)
            return True
        except grpc.RpcError as e:
            return False

    def check_dispatcher_thread(self):
        # периодическая проверка доступности диспетчера в отдельном потоке
        while True:
            with self.lock:
                self.dispatcher_available = self.check_dispatcher_availability()
                if self.dispatcher_available:
                    self.result_received_event.set()
            time.sleep(1)

    def reserve_seat(self, customer_name, seat_number):
        # отправка запроса на бронирование места через диспетчер
        stub = reservation_pb2_grpc.ReservationServiceStub(self.dispatcher_channel)
        reservation_request = reservation_pb2.ReservationRequest(
            customer_name=customer_name,
            seat_number=seat_number
        )
        response = stub.ReserveSeat(reservation_request)
        print(f"Успешность: {response.success}, Сообщение: {response.message}")
        self.result_received_event.set()

    def get_seat_status(self):
        # получение статуса всех мест через диспетчер
        stub = reservation_pb2_grpc.ReservationServiceStub(self.dispatcher_channel)
        seat_status_request = reservation_pb2.SeatListRequest()
        seat_status_response = stub.GetSeatStatus(seat_status_request)
        for status in seat_status_response:
            print(f"Место {status.seat_number}: {status.status}")
        self.result_received_event.set()

    def start(self):
        # запуск потока для проверки доступности диспетчера
        thread = threading.Thread(target=self.check_dispatcher_thread, daemon=True)
        thread.start()

        try:
            while True:
                with self.lock:
                    if self.dispatcher_available:
                        print("1. Просмотр всех комнат и их статус")
                        print("2. Забронировать комнату")
                        print("3. Выход из программы")
                        choice = input("Выберите действие: ")

                        if choice == "1":
                            # вывод статуса всех мест через диспетчер
                            try:
                                self.get_seat_status()
                                self.result_received_event.wait()
                                self.result_received_event.clear()
                            except grpc.RpcError as e:
                                print("Один из узлов сети недоступен. Пожалуйста, подождите...")
                        elif choice == "2":
                            # отправка запроса на бронирование места через диспетчер
                            customer_name = input("Введите ваше имя: ")
                            seat_number = int(input("Введите номер места для бронирования: "))
                            try:
                                self.reserve_seat(customer_name, seat_number)
                                self.result_received_event.wait()
                                self.result_received_event.clear()
                            except grpc.RpcError as e:
                                print("Один из узлов сети недоступен. Пожалуйста, подождите...")
                        elif choice == "3":
                            print("Программа завершена.")
                            break
                        else:
                            print("Некорректный выбор. Пожалуйста, выберите 1, 2 или 3.")
                    else:
                        print("Диспетчер недоступен. Подождите, пожалуйста...")
                        self.result_received_event.clear()
                        time.sleep(5)
        except KeyboardInterrupt:
            print("Клиент завершен")

if __name__ == '__main__':
    client = Client("config.txt")
    client.start()

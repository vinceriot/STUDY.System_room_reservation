import grpc
from concurrent import futures
import reservation_pb2
import reservation_pb2_grpc
import threading
import time


def read_config(config_name):
    config = {}
    with open(config_name, 'r') as file:
        for line in file:
            key, value = map(str.strip, line.split('='))
            config[key] = value
    return config


class SeatManager(reservation_pb2_grpc.ReservationServiceServicer):
    def Ping(self, request, context):
        return reservation_pb2.PingResponse()

    def ReserveSeat(self, request, context):
        # Возвращаем одно и то же сообщение
        return reservation_pb2.ReservationResponse(success=True, message="Временный ответ на ReserveSeat от СУМ 2")

    def GetSeatStatus(self, request, context):
        for seat in range(1, 11):
            status = "Занято"
            yield reservation_pb2.SeatStatus(seat_number=seat, status=status)


def serve_reservation():
    config = read_config("confseatmanserv2.txt")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    reservation_pb2_grpc.add_ReservationServiceServicer_to_server(SeatManager(), server)

    # Запускаем сервер бронирования
    server.add_insecure_port(f"{config['IP']}:{config['Port']}")
    server.start()
    print(f"Сервер управления местами запущен и слушает на {config['IP']}:{config['Port']}...")
    try:
        server.wait_for_termination()

    except KeyboardInterrupt:
        print("Сервер управления местами завершает работу")


if __name__ == '__main__':
    serve_reservation()

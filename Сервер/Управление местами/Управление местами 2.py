import grpc
from concurrent import futures
import reservation_pb2
import reservation_pb2_grpc
import threading
import time
import psycopg2

def read_config(config_name):
    config = {}
    with open(config_name, 'r') as file:
        for line in file:
            key, value = map(str.strip, line.split('='))
            config[key] = value
    return config
class SeatManager(reservation_pb2_grpc.ReservationServiceServicer):
    def __init__(self,db_connection):
        self.db_connection = db_connection

    def Ping(self, request, context):
        return reservation_pb2.PingResponse()

    def ReserveSeat(self, request, context):
        room_id_to_book = request.seat_number
        guest_name_to_set = request.customer_name

        cursor = self.db_connection.cursor()

        try:
            cursor.execute("SELECT roomstatus FROM Rooms WHERE roomid = %s;", (room_id_to_book,))
            current_status = cursor.fetchone()

            if current_status and not current_status[0]:
                cursor.execute("UPDATE Rooms SET roomstatus = true, guestname = %s WHERE roomid = %s;",
                               (guest_name_to_set, room_id_to_book))
                self.db_connection.commit()
                response_success=True
                response_message = f"Комната {room_id_to_book} успешно забронирована для {guest_name_to_set}."
            else:
                response_success = False
                response_message = f"Комната {room_id_to_book} уже занята."

        except Exception as e:
            response_success=False
            response_message = f"Ошибка: {e}"

        finally:
            cursor.close()

        return reservation_pb2.ReservationResponse(success=response_success, message=response_message)

    def GetSeatStatus(self, request, context):
        cursor = self.db_connection.cursor()

        try:
            cursor.execute("SELECT roomid, roomname, CASE WHEN roomstatus THEN 'Занято' ELSE 'Свободно' END, "
                           "CASE WHEN roomstatus THEN guestname ELSE NULL END FROM Rooms;")
            all_rooms = cursor.fetchall()

            for room in all_rooms:
                room_id, room_name, status, guest_name = room
                yield reservation_pb2.SeatStatus(seat_number=room_id, status=status)

        except Exception as e:
            print(f"Ошибка: {e}")

        finally:
            cursor.close()

def serve_reservation():
    config = read_config("confseatmanserv2.txt")

    # Подключение к облачной базе данных
    db_connection = psycopg2.connect("""
        host=###
        port=6432
        dbname=db1
        user=user1
        password=kavalsky945
        target_session_attrs=read-write
    """)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    reservation_pb2_grpc.add_ReservationServiceServicer_to_server(
        SeatManager(db_connection), server
    )

    server.add_insecure_port(f"{config['IP']}:{config['Port']}")
    server.start()
    print(f"Сервер управления местами запущен и слушает на {config['IP']}:{config['Port']}...")

    try:
        server.wait_for_termination()

    except KeyboardInterrupt:
        print("Сервер управления местами завершает работу.")

if __name__ == '__main__':
    serve_reservation()

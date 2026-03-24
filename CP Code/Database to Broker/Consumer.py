import pika
import time
import json
import sys
import os
from InfluxInteraction import Influx

RABBITMQ_BROKER = "XXX.XXX.XXX.XXX"
QUEUE_NAME = "test"
PREFETCH_COUNT = 1
RETRY_DELAY = 5

influx = None


# Raised when the database cannot be reached during message processing
class DatabaseUnavailableError(Exception):
    pass


# Returns the current Influx client or creates one if it does not exist
def get_influx():
    global influx
    if influx is None:
        influx = Influx()
    return influx


# Resets the Influx client so a fresh connection can be created later
def reset_influx():
    global influx
    if influx is not None:
        try:
            influx.exit()
        except Exception:
            pass
    influx = None


# Processes each RabbitMQ message and writes valid payloads into InfluxDB
def callback(ch, method, properties, body):
    global influx

    try:
        payload = json.loads(body.decode())
        print(f"[RECV] tag={method.delivery_tag} redelivered={method.redelivered} payload={payload}")

        if not isinstance(payload, dict):
            print("[REJECT] Invalid payload format")
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        try:
            db = get_influx()
            db.sendtoDB(payload)

            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[ACK] tag={method.delivery_tag} written to DB")

        except Exception as db_error:
            print(f"[DB FAIL] tag={method.delivery_tag} error={db_error}")
            reset_influx()
            raise DatabaseUnavailableError(str(db_error))

    except json.JSONDecodeError as e:
        print(f"[BAD JSON] error={e}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    except DatabaseUnavailableError:
        raise

    except Exception as e:
        print(f"[UNEXPECTED] tag={method.delivery_tag} error={e}")
        raise


# Maintains the RabbitMQ consumer loop and handles reconnect behaviour
def consume():
    while True:
        connection = None
        channel = None

        try:
            print("NEW VERSION LOADED")
            print("[RabbitMQ] Connecting...")

            params = pika.ConnectionParameters(
                host=RABBITMQ_BROKER,
                heartbeat=20,
                blocked_connection_timeout=10
            )

            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.basic_qos(prefetch_count=PREFETCH_COUNT)

            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=callback,
                auto_ack=False
            )

            print("----------------- Waiting for Message -----------------")
            channel.start_consuming()

        except DatabaseUnavailableError as e:
            print(f"[Influx] Database unavailable: {e}")
            print("[RabbitMQ] Closing connection so unacked message is requeued")
            time.sleep(RETRY_DELAY)

        except pika.exceptions.AMQPChannelError as e:
            print(f"[RabbitMQ] Channel error: {e}")
            time.sleep(RETRY_DELAY)

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[RabbitMQ] Connection lost/refused: {e}")
            time.sleep(RETRY_DELAY)

        except KeyboardInterrupt:
            print("Exiting Program...")
            try:
                reset_influx()
            finally:
                try:
                    if channel and channel.is_open:
                        channel.close()
                except Exception:
                    pass

                try:
                    if connection and connection.is_open:
                        connection.close()
                except Exception:
                    pass

            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)

        except Exception as e:
            print(f"[General] error: {e}")
            time.sleep(RETRY_DELAY)

        finally:
            try:
                if channel and channel.is_open:
                    channel.close()
            except Exception:
                pass

            try:
                if connection and connection.is_open:
                    connection.close()
            except Exception:
                pass


# Starts the RabbitMQ consumer when the script is run directly
if __name__ == "__main__":
    consume()

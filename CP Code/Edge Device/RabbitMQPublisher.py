import functools
import logging
import json
import pika
from pika.exchange_type import ExchangeType
from persistqueue import Queue
from persistqueue.exceptions import Empty
from filelock import FileLock

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
              
LOGGER = logging.getLogger(__name__)


class Publisher(object):
      
    EXCHANGE = 'message'
    EXCHANGE_TYPE = ExchangeType.topic
    PUBLISH_INTERVAL = 1  # in secs To modify based on queue handling capabilities
    QUEUE_NAME = 'test'
    ROUTING_KEY = 'example.text'
    _TAILSCALE_IP = 'xxxxxxxxx'
    

    def __init__(self, host = None, port = 5672, username = 'guest', password = 'guest', virtual_host = '/' , heartbeat = 30 ):
        """
        Setup Publisher connection to RabbitMQ broker 

        """
        if host is None:
            host = self._TAILSCALE_IP
						
        self._connection = None
        self._channel = None
        self._queue_dir = "data_queue"
        self._queue = None
        self.lock = FileLock(self._queue_dir + "/.queue.lock")

        self._deliveries = None 
        self._acked = None 
        self._nacked = None 
        self._message_number = None
        self._current_message = None
        self._reconnect_delay = 1
        self._error_ack_count = 0

        self._stopping = False
        
        self._parameters = pika.ConnectionParameters(
			host=host, 		# IP address of broker (check tailscale status)
			port=port, 				
			virtual_host=virtual_host,
			credentials= pika.PlainCredentials(username,password),
			heartbeat=heartbeat, 			# determines how long the message will wait before consider as timeout
			blocked_connection_timeout=30
			)
            
# ======================================== Connection Settings ==========================================                     
    def connect(self):
        
        LOGGER.info('Connecting to %s', self._TAILSCALE_IP)
        return pika.SelectConnection(
            self._parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)


#======================================== Connection Handlers ==========================================       
    
    def on_connection_open(self, _unused_connection):
		
       
        self._reconnect_delay = 1
        #LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        
        #LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(self._reconnect_delay, self._connection.ioloop.stop)
        self._reconnect_delay = min(self._reconnect_delay * 2, 60)

    def on_connection_closed(self, _unused_connection, reason):
        
        
        # Retry Logic 
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
            return
        
        LOGGER.warning('Connection closed, reopening in 5 seconds: %s',   
                           reason)
        self._connection.ioloop.call_later(self._reconnect_delay, self._connection.ioloop.stop)
        self._reconnect_delay = min(self._reconnect_delay * 2, 60)
        
#======================================== Channel Handlers ==========================================   

    def open_channel(self):
        
        #LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        
        #LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)
        self._message_number = 0

    def add_on_channel_close_callback(self):
       
        #LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self._channel = None
        self._current_message = None
        if self._stopping:
            return
        if self._connection is not None and self._connection.is_open:
            self._connection.close()



#======================================== Exchange Handlers ==========================================   

    def setup_exchange(self, exchange_name):
        
        #LOGGER.info('Declaring exchange %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(self.on_exchange_declareok,
                               userdata=exchange_name)
        self._channel.exchange_declare(exchange=exchange_name,
                                       exchange_type=self.EXCHANGE_TYPE,
                                       callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        
        #LOGGER.info('Exchange declared: %s', userdata)
        self.setup_queue(self.QUEUE_NAME)

#======================================== Queue Handlers ========================================== 

    def setup_queue(self, queue_name):
        
        #LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(queue=queue_name,
                                    callback=self.on_queue_declareok,
                                    durable = True)

    def on_queue_declareok(self, _unused_frame):
        
        #LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, self.QUEUE_NAME,
        #            self.ROUTING_KEY)
        self._channel.queue_bind(self.QUEUE_NAME,
                                 self.EXCHANGE,
                                 routing_key=self.ROUTING_KEY,
                                 callback=self.on_bindok)

    def on_bindok(self, _unused_frame):
        
        #LOGGER.info('Queue bound')
        self.start_publishing()
#======================================== Delivery Handlers ========================================== 
    def start_publishing(self):
        
        #LOGGER.info('Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
       
        #LOGGER.info('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        ack_multiple = method_frame.method.multiple
        delivery_tag = method_frame.method.delivery_tag

        LOGGER.info('Received %s for delivery tag: %i (multiple: %s)',
                    confirmation_type, delivery_tag, ack_multiple)

        if confirmation_type == 'ack':
            try:
                with self.lock:
                    self._queue = Queue(self._queue_dir)
                    self._queue.get(block = False)
                    self._queue.task_done()
                self._acked += 1
            
                LOGGER.info("Message ACKED - Data pop out of Queue")
                self._current_message = None
            except Empty:
                self._error_ack_count += 1
                LOGGER.info(f"Ack recieved but empty field, Error Ack Count {self._error_ack_count}")
            except Exception as e:
                LOGGER.error("Error @ delivery: %s",e)
            
            
        elif confirmation_type == 'nack':
            self._nacked += 1
            LOGGER.info("Message NACKED - Data to resend")

        del self._deliveries[delivery_tag]

        if ack_multiple:
            for tmp_tag in list(self._deliveries.keys()):
                if tmp_tag <= delivery_tag:
                    self._acked += 1
                    del self._deliveries[tmp_tag]


    def schedule_next_message(self):
        
        self._connection.ioloop.call_later(self.PUBLISH_INTERVAL,
                                           self.publish_message)

    def publish_message(self):
        
        if self._channel is None or not self._channel.is_open:
            return
        
        if self._current_message is None:
            with self.lock:
                self._queue = Queue(self._queue_dir)
                if self._queue.empty():
                    LOGGER.info("Queue is empty, Skipping publish")
                    self.schedule_next_message()
                    return
                self._current_message = self._queue.get()
        else:
            self.schedule_next_message()
            return
            
 
        properties = pika.BasicProperties(app_id='example-publisher',
                                          content_type='application/json',
                                          delivery_mode=2,
                                          headers={} # can REF hdrs if needed
                                          )
                                          
        self._message_number += 1
        data = self._current_message
        
        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                    data,
                                    properties)
        
        self._deliveries[self._message_number] = True
        self.schedule_next_message()
#======================================== Run Function for System loop ========================================== 
    def run(self):
       
        while not self._stopping:
            self._connection = None
            self._deliveries = {}
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    self._connection.ioloop.start()

        LOGGER.info('Stopped')
#======================================== Disconnetion Handlers ========================================== 
    def stop(self):
        
        LOGGER.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        
        if self._channel is not None:
            LOGGER.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        
        if self._connection is not None and self._connection.is_open:
            LOGGER.info('Closing connection')
            self._connection.close()
    


def main():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

    example = Publisher()
    example.run()


if __name__ == '__main__':
    main()

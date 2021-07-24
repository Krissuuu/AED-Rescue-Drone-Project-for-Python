import pika
import threading

EXCHANGE_NAME = 'drone'

DRONE_ID = 'airsim_simulation'
ROUTING_KEY_WEBRTC = DRONE_ID + ".phone.webrtc"
BINDING_KEY_WEBRTC = DRONE_ID + ".web.webrtc"

class RabbitMQ_AirSim():

    def __init__(self):
        auth = pika.PlainCredentials('aiotlab', 'aiotlab208')
        parameters= pika.ConnectionParameters(host='aiotlab-drone-cloud.ga', port=5672, virtual_host='/', credentials=auth)
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')

    def publish(self, message):
        self.channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY_WEBRTC, body=message)

    def consume(self, callback):
        result = self.channel.queue_declare('AirSim_Simulation_WebRTC', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=BINDING_KEY_WEBRTC)

        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()
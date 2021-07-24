import pika
from analyze_web_command import analyze_web_json

EXCHANGE_NAME = 'drone'

DRONE_ID = 'airsim_simulation'
ROUTING_KEY_DRONE = DRONE_ID + ".phone.drone"
BINDING_KEY_DRONE = DRONE_ID + ".web.drone"

class RabbitMQ():

    def __init__(self, vehicle=None):
        auth = pika.PlainCredentials('aiotlab', 'aiotlab208')
        parameters= pika.ConnectionParameters(host='aiotlab-drone-cloud.ga', port=5672, virtual_host='/', credentials=auth)
        connection = pika.BlockingConnection(parameters)
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')

        self.vehicle = vehicle

    def publish(self, message):
        self.channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY_DRONE, body=message)

    def consume(self):
        result = self.channel.queue_declare('AirSim_Simulation_Drone', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key=BINDING_KEY_DRONE)

        def callback(ch, method, properties, body):
            if method.routing_key == BINDING_KEY_DRONE:
                # print("%r:%r" % ("[WEB_CMD]", body))
                analyze_web_json(self.vehicle, body)

        self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
        self.channel.start_consuming()
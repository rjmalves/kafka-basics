from tweepy import OAuthHandler, API  # type: ignore
from tweepy import StreamListener, Stream, Status  # type: ignore
from kafka import KafkaProducer  # type: ignore
import logging
import json
from queue import SimpleQueue


class TwitterListener(StreamListener):
    """
    """
    def add_queue(self, queue: SimpleQueue):
        self.queue = queue

    def on_status(self, status):
        self.queue.put(status)


class TwitterProducer:
    """
    """
    # Configuração básica do logger
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)

    def __init__(self):
        # Autenticação com a API do Twitter
        self.key = ''
        self.secret = ''
        self.token_key = ''
        self.token_secret = ''
        self.auth = OAuthHandler(self.key, self.secret)
        self.auth.set_access_token(self.token_key, self.token_secret)
        self.api = API(self.auth)
        # Criação do Producer do Kafka
        self.servers = ["kafka-server:9092"]
        self.value_ser = lambda m: m.encode('utf-8')
        self.producer = KafkaProducer(bootstrap_servers=self.servers,
                                      value_serializer=self.value_ser,
                                      acks="all",
                                      retries=2e32 - 1,
                                      max_in_flight_requests_per_connection=5,
                                      compression_type="gzip",
                                      batch_size=32 * 1024,
                                      linger_ms=20)
        # Criação da fila para armazenar os elementos já obtidos do twitter,
        # mas ainda não enviados ao Kafka
        self.queue = SimpleQueue()
        # Criação da stream de dados do Twitter
        self.listener = TwitterListener()
        self.listener.add_queue(self.queue)
        self.stream = Stream(auth=self.api.auth, listener=self.listener)
        # Configuração do Logger
        self.logger = logging.getLogger("twitter_logger")
        self.logger.setLevel(logging.WARNING)
        self.logger.warning("TwitterProducer iniciado.")

    def run(self):
        """
        """
        try:
            # Inicia a stream de dados do twitter
            self.stream.filter(track=["thelma"], is_async=True)

            # Define a função de callback do envio de mensagens
            def on_send(metadata):
                logger = logging.getLogger("twitter_logger")
                logger.warning("{}, {}, {}".format(metadata.topic,
                                                   metadata.partition,
                                                   metadata.offset))
            while True:
                if not self.queue.empty():
                    status: Status = self.queue.get()
                    self.producer.send("twitter_tweets",
                                       json.dumps(status._json)
                                       ).add_callback(on_send)
        except KeyboardInterrupt:
            self.logger.warning("Aplicação interrompida pelo usuário.")
        except Exception as e:
            self.logger.warning("Algo deu errado: ", e)

    def __del__(self):
        """
        """
        self.stream.disconnect()
        self.producer.flush()
        self.logger.warning("TwitterProducer finalizado.")

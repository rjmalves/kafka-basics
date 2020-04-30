from kafka import KafkaProducer  # type: ignore
import logging


class ProducerDemo:
    """
    Exemplo de agregação de um produtor kafka com algumas configurações.
    """
    # Configuração básica do logger
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)

    def __init__(self):
        # Argumento obrigatório na criação do producer
        self.servers = ["kafka-server:9092"]
        # key é uma chave para roteamento entre partições
        self.key_ser = lambda k: k.encode('ascii')
        # value é a mensagem em si.
        self.value_ser = lambda m: m.encode('ascii')
        # Cria o producer em si (pode ter mais parâmetros: acks, etc.)
        self.producer = KafkaProducer(bootstrap_servers=self.servers,
                                      key_serializer=self.key_ser,
                                      value_serializer=self.value_ser)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)
        self.logger.warning("ProducerDemo iniciado.")

    def produce(self, topic: str, msg: str, key: str):
        """
        Envia uma mensagem para um tópico.
        """
        def on_send_success(metadata):
            print("Top.: {}, Part.: {}, Offset: {}".format(metadata.topic,
                                                           metadata.partition,
                                                           metadata.offset))
        print("Key: {}".format(key))
        self.producer.send(topic,
                           key=key,
                           value=msg).add_callback(on_send_success)

    def __del__(self):
        """
        Adiciona o comando extra de flush para garantir o envio.
        """
        self.producer.flush()
        self.logger.warning("ProducerDemo finalizado.")

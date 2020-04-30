from kafka import KafkaConsumer  # type: ignore
import logging


class ConsumerDemo:
    """
    Exemplo de agregação de um consumidor kafka com algumas configurações.
    """
    # Configuração básica do logger
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)

    def __init__(self):
        # Argumento obrigatório na criação do consumer
        self.servers = ["kafka-server:9092"]
        # key é uma chave para roteamento entre partições
        self.key_des = lambda k: k.decode('ascii')
        # value é a mensagem em si.
        self.value_des = lambda m: m.decode('ascii')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)
        self.logger.warning("ConsumerDemo iniciado.")

    def consume(self, topic: str, group_id: str):
        """
        Escuta mensagens em um tópico.
        """
        # Cria o consumer em si (pode ter mais parâmetros)
        try:
            self.consumer = KafkaConsumer(topic,
                                          group_id=group_id,
                                          bootstrap_servers=self.servers,
                                          key_deserializer=self.key_des,
                                          value_deserializer=self.value_des,
                                          auto_offset_reset="earliest",
                                          consumer_timeout_ms=10000)
            for message in self.consumer:
                self.logger.warning("Top: {},Par: {},Off: {},Key: {},Val: {}"
                                    .format(message.topic,
                                            message.partition,
                                            message.offset,
                                            message.key,
                                            message.value))
        except KeyboardInterrupt:
            self.consumer.close()
            self.logger.warning("Finalizado pelo teclado!")
        except Exception as e:
            self.consumer.close()
            self.logger.warning("Peguei a excecao {}".format(e))

    def __del__(self):
        self.logger.warning("ConsumerDemo finalizado.")

from kafka import KafkaConsumer, TopicPartition  # type: ignore
import logging


class ConsumerAssignSeek:
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
        self.logger.warning("ConsumerAssignSeek iniciado.")

    def consume(self, topic: str, partition: int):
        """
        Escuta mensagens em um tópico.
        """
        try:
            # Cria o consumer em si, mas sem inscrever em nada
            self.consumer = KafkaConsumer(bootstrap_servers=self.servers,
                                          key_deserializer=self.key_des,
                                          value_deserializer=self.value_des,
                                          auto_offset_reset="earliest",
                                          consumer_timeout_ms=10000)
            # Realiza a inscrição manualmente (assign)
            partition = TopicPartition(topic, partition)
            self.consumer.assign([partition])
            # Ajusta o offset de início da busca (seek)
            self.consumer.seek(partition, 15)
            # Parâmetros da escuta de mensagens a ser feita
            messages_to_read = 5
            keep_reading = True
            messages_read = 0
            # Escuta as mensagens
            while keep_reading:
                self.consumer.poll(timeout_ms=0)
                for message in self.consumer:
                    messages_read += 1
                    self.logger.warning("To: {},Pa: {},Of: {},Key: {},Val: {}"
                                        .format(message.topic,
                                                message.partition,
                                                message.offset,
                                                message.key,
                                                message.value))
                    if messages_read >= messages_to_read:
                        keep_reading = False
                        break
        except KeyboardInterrupt:
            self.consumer.close()
            self.logger.warning("Finalizado pelo teclado!")
        except Exception as e:
            self.consumer.close()
            self.logger.warning("Peguei a excecao {}".format(e))

    def __del__(self):
        self.logger.warning("ConsumerAssignSeek finalizado.")

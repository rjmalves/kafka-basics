from elasticsearch import Elasticsearch, helpers  # type: ignore
from kafka import KafkaConsumer  # type: ignore
from typing import List
import logging
import json


class ElasticSearchConsumer:
    """
    """
    # Configuração básica do logger
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(format=FORMAT)

    def __init__(self):
        # Configurações de acesso ao ElasticSearch
        self.hosts = ["kafka_basics_elasticsearch_1:9200"]
        self.es = Elasticsearch(hosts=self.hosts)
        # Argumento obrigatório na criação do consumer
        self.servers = ["kafka-server:9092"]
        # value é a mensagem em si.
        self.value_des = lambda m: m.decode('utf-8')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.WARNING)
        self.logger.warning("ElasticSearchConsumer iniciado.")

    def consume(self, topic: str, group_id: str):
        """
        """
        try:
            # Cria o consumer em si
            self.consumer = KafkaConsumer(group_id=group_id,
                                          bootstrap_servers=self.servers,
                                          value_deserializer=self.value_des,
                                          auto_offset_reset="earliest",
                                          enable_auto_commit=False,
                                          max_poll_records=100)
            # Faz a inscrição no tópico
            self.consumer.subscribe([topic])
            # Lê as mensagens até que a aplicação seja interrompida
            while True:
                # Faz o polling, tolerando até 100 ms de espera
                messages = self.consumer.poll(timeout_ms=100)
                # Prepara as ações para o ElasticSearch
                actions: List[dict] = []
                # Quando usa o generator, faz um novo poll a cada iteração
                # Não pode usar em um loop, portanto.
                for __, consumer_records in messages.items():
                    for consumer_record in consumer_records:
                        parsed_message = json.loads(consumer_record.value)
                        # Obtém o ID do tweet
                        message_id: str = parsed_message["id"]
                        # Descreve uma action
                        action = {}
                        action["_index"] = "twitter"
                        action["_type"] = "tweets"
                        action["_id"] = message_id
                        action["_source"] = parsed_message
                        actions.append(action)
                    helpers.bulk(self.es, actions)
                    # Informa o usuário
                    self.logger.warning("Top: {}, Num. Tweets: {}, Id[0]: {}"
                                        .format(consumer_record.topic,
                                                len(actions),
                                                actions[0]["_id"]))
                # Após processar as mensagens, faz um commit bloqueante
                self.consumer.commit()
        except KeyboardInterrupt:
            self.consumer.close()
            self.logger.warning("Finalizado pelo teclado!")
        except Exception as e:
            self.consumer.close()
            self.logger.warning("Peguei a excecao {}".format(e))

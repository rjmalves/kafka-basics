import sys
from tutorial1.producerdemo import ProducerDemo
from tutorial1.consumerassignseek import ConsumerAssignSeek
from tutorial2.twitterproducer import TwitterProducer
from tutorial2.elasticsearchconsumer import ElasticSearchConsumer


def producer_demo():
    try:
        producer = ProducerDemo()
        for i in range(10):
            producer.produce("first_topic",
                             "Hello, world! " + str(i),
                             str(i % 2))
        exit(0)
    except KeyboardInterrupt:
        print("Interrompida pelo usuário!")
        exit(1)
    except Exception as e:
        print("Algo deu errado: ", e)
        exit(1)


def consumer_demo():
    try:
        consumer = ConsumerAssignSeek()
        consumer.consume("first_topic", 0)
        exit(0)
    except KeyboardInterrupt:
        print("Interrompida pelo usuário!")
        exit(1)
    except Exception as e:
        print("Algo deu errado: ", e)
        exit(1)


def twitter_producer():
    try:
        producer = TwitterProducer()
        producer.run()
        exit(0)
    except KeyboardInterrupt:
        print("Interrompida pelo usuário!")
        exit(1)
    except Exception as e:
        print("Algo deu errado: ", e)
        exit(1)


def elasticsearch_consumer():
    try:
        consumer = ElasticSearchConsumer()
        consumer.consume("twitter_tweets", "kafka-twitter-elasticsearch")
        exit(0)
    except KeyboardInterrupt:
        print("Interrompida pelo usuário!")
        exit(1)
    except Exception as e:
        print("Algo deu errado: ", e)
        exit(1)


if __name__ == "__main__":
    # Precisa especificar o comando de inicialização!
    if len(sys.argv) < 2:
        print("Por favor especifique o comando de inicialização!")
        exit(1)
    # Senão, se o comando foi passado:
    arg = sys.argv[1]
    if arg == "--producer-demo":
        producer_demo()
    elif arg == "--consumer-demo":
        consumer_demo()
    elif arg == "--twitter-producer":
        twitter_producer()
    elif arg == "--elasticsearch-consumer":
        elasticsearch_consumer()
    else:
        print("Comando não reconhecido!")
        exit(1)

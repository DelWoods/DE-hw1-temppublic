# from abc import abstractmethod
import json
import logging
import os
import sys
import time

# from ..producer import ToxicityProducer, load_source_config
# from src.utils import load_source_config
from utils import load_source_config
from kafka import KafkaProducer

# from src.logger import setup_logger
from logger import setup_logger


setup_logger()
logger = logging.getLogger(__name__)


class ToxicityProducer:
    def __init__(self, kafka_cfg: str, source_cfg: str):
        
        self.config = self.load_config(kafka_cfg)
        self.custom_config = self.load_config(source_cfg)

        # basic params of the kafka config: 
        # env has priority, e.g. localhost:9092 for the notebook outside docker: 
        self.kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP', self.config['kafka_bootstrap'])
        self.fetch_interval_s = self.config.get('fetch_interval_s')
        self.batch_size = self.config.get('batch_size')
        
        if self.batch_size is None: 
            self.batch_size = self.custom_config.get('batch_size')
            self.fetch_interval_s = self.custom_config.get('fetch_interval_s')

        self.kafka_retries = self.config.get('kafka_retries', 5)
        self.kafka_retry_delay = self.config.get('kafka_retry_delay', 5)

        self.kafka_topic = self.custom_config.get('kafka_topic')
        self.kafka_partition = self.custom_config.get('kafka_partition')

        self.producer = None
        self.default_topic = self.kafka_topic

        # additional params of the source config: 
        # self.source_config = self.load_config(source_cfg_path)

        self.create_kafka_producer()
        
    def load_config(self, config_path: str) -> dict:
        return load_source_config(config_path)
    
    def create_kafka_producer(self):
        
        for attempt in range(1, self.kafka_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                )
                logger.info(f"Connected to Kafka at {self.kafka_bootstrap}")

                # setup custom topic, if it was created: 
                self.setup_custom_topic(self.kafka_topic)

                return self.producer
            
            except Exception as e:
                logger.warning(f"Kafka attempt {attempt}/{self.kafka_retries} failed: {e}")
                if attempt < self.kafka_retries:
                    time.sleep(self.kafka_retry_delay)
                    
    def setup_custom_topic(self, topic): 
        
        # choosing custom topic: 
        if topic is not None: 
            # check whether Kafka topic exists: 
            if not self.check_kafka_topic_exists_with_producer(self.producer, topic):
                logger.error(f"Kafka topic '{topic}' does not exist. Using the tpoic by default.")
                # raise ValueError(f"Kafka topic '{topic}' does not exist.")
            else: 
                self.default_topic = topic

    def check_kafka_topic_exists_with_producer(self, producer: KafkaProducer, topic_name: str) -> bool:
        """Check if a Kafka topic exists using the KafkaProducer."""
        try:
            # Fetch metadata for all topics
            metadata = producer.bootstrap_connected()
            if not metadata:
                logger.error("Producer is not connected to Kafka.")
                return False

            # Check if the topic exists in the metadata
            topics = producer.partitions_for(topic_name)
            return topics is not None
        except Exception as e:
            logger.error(f"Error checking Kafka topic existence with producer: {e}")
            return False

    def produce_message(self, text: str, topic: str = None, extra: dict = None):
        """Send {'text': text, **extra} to the topic (extra: e.g. raw_text_id)"""
        
        s_topic = topic
        part = self.kafka_partition
        if topic is None: 
            s_topic = self.default_topic    
        
        # logger.info(f'topic {s_topic} text {text} part {part}')
        value = {'text': text}
        if extra: 
            value.update(extra)
        self.producer.send(s_topic, value=value, partition=part)
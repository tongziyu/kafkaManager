# -*- coding: utf-8 -*-
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import KafkaError, TopicAlreadyExistsError, NoBrokersAvailable
from kafka.structs import TopicPartition
import logging


class KafkaManager:
    def __init__(self, bootstrap_servers, security_protocol=None, 
                 sasl_mechanism=None, sasl_plain_username=None, 
                 sasl_plain_password=None, ssl_cafile=None,
                 ssl_certfile=None, ssl_keyfile=None,
                 client_config_overrides=None,
                 consumer_config_overrides=None,
                 **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self.bootstrap_servers_list = self._normalize_bootstrap_servers(bootstrap_servers)
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_plain_username = sasl_plain_username
        self.sasl_plain_password = sasl_plain_password
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.admin_client = None
        self.logger = logging.getLogger(__name__)
        self.client_config_overrides = client_config_overrides or {}
        self.consumer_config_overrides = consumer_config_overrides or {}
        # 兼容遗留参数
        self.legacy_overrides = kwargs
        # 添加topic详情缓存，避免频繁请求
        self.topic_details_cache = {}
        self.cache_timeout = 30  # 缓存30秒
        self.connect()

    def _normalize_bootstrap_servers(self, servers):
        """确保 bootstrap_servers 以列表形式存在"""
        if not servers:
            return []
        if isinstance(servers, (list, tuple, set)):
            return [str(s).strip() for s in servers if str(s).strip()]
        # 字符串形式，按逗号分割
        return [segment.strip() for segment in str(servers).split(',') if segment.strip()]

    def _build_client_config(self, include_client_id=True):
        config = {
            'bootstrap_servers': self.bootstrap_servers_list or self.bootstrap_servers,
        }

        if include_client_id:
            config['client_id'] = 'kafka_manager'

        if self.security_protocol:
            config['security_protocol'] = self.security_protocol

            if self.security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
                if self.sasl_mechanism:
                    config['sasl_mechanism'] = self.sasl_mechanism
                if self.sasl_plain_username and self.sasl_plain_password:
                    config['sasl_plain_username'] = self.sasl_plain_username
                    config['sasl_plain_password'] = self.sasl_plain_password

            if self.security_protocol in ['SSL', 'SASL_SSL']:
                if self.ssl_cafile:
                    config['ssl_cafile'] = self.ssl_cafile
                if self.ssl_certfile:
                    config['ssl_certfile'] = self.ssl_certfile
                if self.ssl_keyfile:
                    config['ssl_keyfile'] = self.ssl_keyfile

        combined_overrides = {}
        combined_overrides.update(self.client_config_overrides)
        combined_overrides.update(self.legacy_overrides)

        for key, value in combined_overrides.items():
            if value in (None, '', False):
                continue
            if key in ['bootstrap_servers', 'security_protocol', 'sasl_mechanism',
                       'sasl_plain_username', 'sasl_plain_password',
                       'ssl_cafile', 'ssl_certfile', 'ssl_keyfile'] and key in config:
                continue
            config[key] = value

        return config

    def connect(self):
        """连接到Kafka集群"""
        try:
            client_config = self._build_client_config(include_client_id=True)
            self.admin_client = KafkaAdminClient(**client_config)
            self.logger.info(f"成功连接到Kafka: {self.bootstrap_servers}")
        except NoBrokersAvailable as e:
            self.logger.error(f"无法连接到Kafka: {self.bootstrap_servers}，未发现可用Broker")
            raise
        except Exception as e:
            self.logger.error(f"连接Kafka失败: {str(e)}")
            raise

    def list_topics(self):
        """获取Topic列表"""
        try:
            return list(self.admin_client.list_topics())
        except Exception as e:
            self.logger.error(f"获取Topic列表失败: {str(e)}")
            raise

    def get_client_config(self):
        """获取客户端配置（用于创建Consumer等）"""
        return self._build_client_config(include_client_id=True)

    def create_consumer(self, topic, group_id=None, auto_offset_reset='latest', **kwargs):
        """创建Kafka消费者"""
        try:
            client_config = self.get_client_config()
            
            # 消费者特定配置
            consumer_config = {
                'auto_offset_reset': auto_offset_reset,
                'enable_auto_commit': False,
                'value_deserializer': lambda x: x,
                'key_deserializer': lambda x: x,
            }
            
            if group_id:
                consumer_config['group_id'] = group_id
            else:
                # 如果没有指定group_id，使用唯一的group_id避免冲突
                import uuid
                consumer_config['group_id'] = f'kafka_manager_{uuid.uuid4().hex[:8]}'
            
            # 合并配置
            consumer_config.update(client_config)
            for key, value in self.consumer_config_overrides.items():
                if value in (None, ''):
                    continue
                consumer_config[key] = value
            consumer_config.update(kwargs)

            consumer = KafkaConsumer(topic, **consumer_config)
            self.logger.info(f"创建消费者成功，topic: {topic}")
            return consumer
        except Exception as e:
            self.logger.error(f"创建消费者失败: {str(e)}")
            raise

    def create_topic(self, name, num_partitions=1, replication_factor=1, configs=None, validate_only=False):
        """创建新的Topic"""
        if not self.admin_client:
            self.connect()

        topic = NewTopic(
            name=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs=configs or {}
        )

        # 创建前清掉旧缓存
        if name in self.topic_details_cache:
            del self.topic_details_cache[name]

        try:
            futures = self.admin_client.create_topics([topic], validate_only=validate_only)
            for topic_name, future in futures.items():
                future.result()
                self.logger.info(f"创建Topic成功: {topic_name}")
        except TopicAlreadyExistsError:
            self.logger.warning(f"Topic已存在: {name}")
            raise
        except KafkaError as e:
            self.logger.error(f"创建Topic失败: {str(e)}")
            raise

    def get_topic_details(self, topic_name):
        """获取Topic详情"""
        import time

        current_time = time.time()
        cached = self.topic_details_cache.get(topic_name)
        if cached:
            cached_data, cache_time = cached
            if current_time - cache_time < self.cache_timeout:
                return cached_data.copy()

        if not self.admin_client:
            self.connect()

        try:
            descriptions = self.admin_client.describe_topics([topic_name])
            if not descriptions:
                return None

            topic_meta = descriptions[0]
            if topic_meta.get('error_code'):
                self.logger.error(f"Topic {topic_name} 描述失败，错误码: {topic_meta['error_code']}")
                return None

            details = {
                'name': topic_meta.get('topic', topic_name),
                'is_internal': topic_meta.get('is_internal', False),
                'partition_count': len(topic_meta.get('partitions', [])),
                'replication_factor': 0,
                'configs': {},
                'partitions': [],
                'message_count': 0
            }

            # 获取配置详情
            try:
                config_resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
                config_result = self.admin_client.describe_configs([config_resource])
                resource_configs = None
                if isinstance(config_result, dict):
                    resource_configs = config_result.get(config_resource)
                else:
                    for resource, configs in config_result:
                        if resource == config_resource:
                            resource_configs = configs
                            break
                if resource_configs:
                    details['configs'] = {name: entry.value for name, entry in resource_configs.items()}
            except Exception as config_error:
                self.logger.warning(f"获取Topic配置失败: {config_error}")

            partitions = topic_meta.get('partitions', [])
            if partitions:
                first_replicas = partitions[0].get('replicas', [])
                details['replication_factor'] = len(first_replicas) if first_replicas else 0

                topic_partitions = [TopicPartition(topic_name, p['partition']) for p in partitions]
                offsets = {}
                consumer = None
                try:
                    consumer_config = self._build_client_config(include_client_id=True)
                    consumer_config.update({
                        'enable_auto_commit': False,
                        'auto_offset_reset': 'earliest',
                        'consumer_timeout_ms': 1000,
                        'client_id': 'kafka_manager_topic_inspector'
                    })
                    consumer = KafkaConsumer(topic_name, **consumer_config)
                    consumer.poll(timeout_ms=0)
                    beginning_offsets = consumer.beginning_offsets(topic_partitions)
                    end_offsets = consumer.end_offsets(topic_partitions)
                    offsets = {
                        tp.partition: {
                            'earliest': beginning_offsets.get(tp, 0),
                            'latest': end_offsets.get(tp, 0)
                        }
                        for tp in topic_partitions
                    }
                except Exception as offset_error:
                    self.logger.warning(f"计算Topic {topic_name} 偏移量失败: {offset_error}")
                finally:
                    if consumer:
                        consumer.close()

                for partition_meta in partitions:
                    partition_id = partition_meta.get('partition')
                    partition_offsets = offsets.get(partition_id, {'earliest': 0, 'latest': 0})
                    message_count = max(partition_offsets['latest'] - partition_offsets['earliest'], 0)
                    details['message_count'] += message_count
                    details['partitions'].append({
                        'partition_id': partition_id,
                        'leader': partition_meta.get('leader'),
                        'replicas': partition_meta.get('replicas', []),
                        'isr': partition_meta.get('isr', []),
                        'earliest_offset': partition_offsets['earliest'],
                        'latest_offset': partition_offsets['latest'],
                        'message_count': message_count
                    })

            self.topic_details_cache[topic_name] = (details.copy(), current_time)
            return details
        except Exception as e:
            self.logger.error(f"获取Topic详情失败: {str(e)}")
            return None
    
    def delete_topic(self, topic_name):
        """删除Topic
        
        Args:
            topic_name: 要删除的Topic名称
        """
        # 删除topic时清除缓存
        if topic_name in self.topic_details_cache:
            del self.topic_details_cache[topic_name]
        
        try:
            futures = self.admin_client.delete_topics([topic_name])
            for topic, future in futures.items():
                future.result()
                self.logger.info(f"删除Topic成功: {topic}")
        except Exception as e:
            self.logger.error(f"删除Topic失败: {str(e)}")
            raise

    def close(self):
        """关闭连接"""
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None
            self.logger.info("Kafka连接已关闭")
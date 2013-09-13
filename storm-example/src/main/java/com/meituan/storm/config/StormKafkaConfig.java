package com.meituan.storm.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormKafkaConfig {
	private static final Logger logger = LoggerFactory.getLogger(StormKafkaConfig.class);
	
	public boolean runLocalMode = false;
	public boolean runDebugMode = false;
	
	public String kafkaZkServers = null;
	public String kafkaZkBrokerPath = null;
	
	public String topologyName = null;
	public String topologyZkRoot = null;
	public Integer topologyNumWorkers = 0;
	
	public String consumerTopic = null;
	public long consumerTopicStartOffset = -1;
	public String consumerGroupId = null;
	
	public boolean loadFromFile(String configFile) {
		Properties prop = new Properties();
		try {
			FileInputStream fis = new FileInputStream(configFile);
			prop.load(fis);
		} catch (FileNotFoundException fnfe) {
			logger.error("StormKafkaConfig.loadFromFile ["+configFile+"] not find.");
			return false;
		} catch (IOException ioe) {
			logger.error("StormKafkaConfig.loadFromFile ["+configFile+"] error: " + ioe);
			return false;			
		} 
		
		String tmpStr = prop.getProperty("run_local_mode", null);
		if (tmpStr != null && tmpStr.equals("1")) {
			runLocalMode = true;
		}
		tmpStr = prop.getProperty("run_debug_mode", null);
		if (tmpStr != null && tmpStr.equals("1")) {
			runDebugMode = true;
		}
		
		kafkaZkServers = prop.getProperty("kafka_zk_servers", null);
		if (kafkaZkServers == null || kafkaZkServers.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile kafka_zk_servers is empty");
			return false;
		}
		
		kafkaZkBrokerPath = prop.getProperty("kafka_zk_broker_path", null);
		if (kafkaZkBrokerPath == null || kafkaZkBrokerPath.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile kafka_zk_broker_path is empty");
			return false;
		}
		
		topologyName = prop.getProperty("topology_name", null);
		if (topologyName == null || topologyName.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile topology_name is empty");
			return false;
		}
		
		topologyZkRoot = prop.getProperty("topology_zk_root", null);
		if (topologyZkRoot == null || topologyZkRoot.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile topology_zk_root is empty");
			return false;
		}
		
		String topologyNumWorkersStr = prop.getProperty("topology_num_workers", null);
		if (topologyNumWorkersStr == null || topologyNumWorkersStr.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile topology_num_workers is empty");
			return false;
		}
		topologyNumWorkers = Integer.valueOf(topologyNumWorkersStr);
		if (topologyNumWorkers <= 0 || topologyNumWorkers > 512) {
			logger.error("StormKafkaConfig.loadFromFile topologyNumWorkers["+topologyNumWorkers+"] illegical.");
			return false;
		}
		
		consumerTopic = prop.getProperty("consumer_topic", null);
		if (consumerTopic == null || consumerTopic.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile consumer_topic is empty");
			return false;
		}
		
		String consumerTopicStartOffsetStr = prop.getProperty("consumer_topic_start_offset", null);
		if (consumerTopicStartOffsetStr == null || consumerTopicStartOffsetStr.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile consumer_topic_start_offset is empty");
			return false;
		}
		consumerTopicStartOffset = Long.valueOf(consumerTopicStartOffsetStr);
		
		consumerGroupId = prop.getProperty("consumer_group_id", null);
		if (consumerGroupId == null || consumerGroupId.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile consumer_group_id is empty");
			return false;
		}		
		
		return true;
	}
}

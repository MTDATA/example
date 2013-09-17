package com.meituan.storm.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormRabbitMQConfig {
	private static final Logger logger = LoggerFactory.getLogger(StormRabbitMQConfig.class);
	
	public boolean runLocalMode = false;
	public boolean runDebugMode = false;
	
	public String stormHost = null;
	public Integer stormPort = 5672;
	public String stormUsername = null;
	public String stormPassword = null;
	public String stormVhost = null;
	public String stormExchange = null;
	public String stormRoutekey = null;
	
	public String topologyName = null;
	public Integer topologyNumWorkers = 0;
	
	public boolean loadFromFile(String configFile) {
		Properties prop = new Properties();
		try {
			FileInputStream fis = new FileInputStream(configFile);
			prop.load(fis);
		} catch (FileNotFoundException fnfe) {
			logger.error("StormRabbitMQConfig.loadFromFile ["+configFile+"] not find.");
			return false;
		} catch (IOException ioe) {
			logger.error("StormRabbitMQConfig.loadFromFile ["+configFile+"] error: " + ioe);
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
		
		stormHost = prop.getProperty("storm_host", null);
		if (stormHost == null || stormHost.length() == 0) {
			logger.error("StormRabbitMQConfig.loadFromFile storm_host is empty");
			return false;
		}
		
		String stormPortStr = prop.getProperty("storm_port", null);
		if (stormPortStr == null || stormPortStr.length() == 0) {
			logger.error("StormRabbitMQConfig.loadFromFile storm_port is empty");
			return false;
		}
		stormPort = Integer.valueOf(stormPortStr);
		if (stormPort <= 1024 || stormPort >= 65565) {
			logger.error("StormKafkaConfig.loadFromFile topologyNumWorkers["+topologyNumWorkers+"] illegical.");
			return false;
		}
		
		stormUsername = prop.getProperty("storm_uesrname", null);
		if (stormUsername == null || stormUsername.length() == 0) {
			logger.error("StormRabbitMQConfig.loadFromFile storm_uesrname is empty");
			return false;
		}
		
		stormPassword = prop.getProperty("storm_password", null);
		if (stormPassword == null || stormPassword.length() == 0) {
			logger.error("StormRabbitMQConfig.loadFromFile storm_password is empty");
			return false;
		}
		
		stormVhost = prop.getProperty("storm_vhost", null);
		if (stormVhost == null || stormVhost.length() == 0) {
			logger.error("StormRabbitMQConfig.loadFromFile storm_vhost is empty");
			return false;
		}
		
		stormExchange = prop.getProperty("storm_exchange", null);
		if (stormExchange == null || stormExchange.length() == 0) {
			logger.error("StormRabbitMQConfig.loadFromFile storm_exchange is empty");
			return false;
		}
		
		stormRoutekey = prop.getProperty("storm_routekey", null);
		if (stormRoutekey == null || stormRoutekey.length() == 0) {
			logger.error("StormRabbitMQConfig.loadFromFile storm_routekey is empty");
			return false;
		}
		
		topologyName = prop.getProperty("topology_name", null);
		if (topologyName == null || topologyName.length() == 0) {
			logger.error("StormKafkaConfig.loadFromFile topology_name is empty");
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
				
		return true;
	}
}

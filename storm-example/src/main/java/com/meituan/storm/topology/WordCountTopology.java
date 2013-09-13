package com.meituan.storm.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.KafkaConfig.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.meituan.storm.bolt.SplitBolt;
import com.meituan.storm.bolt.WordCountBolt;
import com.meituan.storm.config.StormKafkaConfig;

public class WordCountTopology {
	private static final Logger logger = LoggerFactory.getLogger(WordCountTopology.class);
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 1) {
			logger.error("Usage: storm jar example-1.0.0-jar-with-dependencies.jar \\" +
									"com.meituan.storm.topology.WordCountTopology \\" +
									"/opt/meituan/storm_topology_config/WordCountTopology.proprites");
			System.exit(-1);
		}
		
		//1. load config
		StormKafkaConfig skc = new StormKafkaConfig();
		if (skc.loadFromFile(args[0]) == false) {
			logger.error("load config file[" + args[0] + "] error");
			System.exit(-1);
		}
		
		//2. create spout
		SpoutConfig spoutConfig = new SpoutConfig(
                new ZkHosts(skc.kafkaZkServers, skc.kafkaZkBrokerPath), 
                skc.consumerTopic, skc.topologyZkRoot, skc.consumerGroupId);
	    //spoutConfig.zkServers = zkServers;
        //spoutConfig.zkPort = 2181;
		spoutConfig.scheme = new StringScheme(); 
        spoutConfig.forceStartOffsetTime(skc.consumerTopicStartOffset);
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		//2. create topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", kafkaSpout, 1);
		builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("spout");
		builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));

		//3. submit topology
		Config conf = new Config();
		conf.setDebug(skc.runDebugMode);
		if (skc.runLocalMode) {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(skc.topologyName, conf, builder.createTopology());
			Thread.sleep(300000);
			cluster.shutdown();
		} else {
			conf.setNumWorkers(skc.topologyNumWorkers);
			StormSubmitter.submitTopology(skc.topologyName, conf, builder.createTopology());
		}
	}
}

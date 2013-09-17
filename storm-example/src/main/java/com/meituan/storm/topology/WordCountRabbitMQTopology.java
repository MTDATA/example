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
import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.meituan.storm.bolt.SplitBolt;
import com.meituan.storm.bolt.WordCountBolt;
import com.meituan.storm.config.StormKafkaConfig;
import com.meituan.storm.config.StormRabbitMQConfig;
import com.rapportive.storm.amqp.ExclusiveQueueWithBinding;
import com.rapportive.storm.amqp.QueueDeclaration;
import com.rapportive.storm.spout.AMQPSpout;

public class WordCountRabbitMQTopology {
	private static final Logger logger = LoggerFactory.getLogger(WordCountRabbitMQTopology.class);
	
	public static String host = "10.64.32.26";
	public static int port = 5672;
	public static String username = "storm";
	public static String passwd = "storm123";
	public static String vhost = "/originallog";
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 1) {
			logger.error("Usage: storm jar example-1.0.0-jar-with-dependencies.jar \\" +
									"com.meituan.storm.topology.WordCountTopology \\" +
									"/opt/meituan/storm_topology_config/WordCountTopology.proprites");
			System.exit(-1);
		}
		
		//1. load config
		StormRabbitMQConfig skc = new StormRabbitMQConfig();
		if (skc.loadFromFile(args[0]) == false) {
			logger.error("load config file[" + args[0] + "] error");
			System.exit(-1);
		}
		
		//2. create spout
		QueueDeclaration queueDeclaration = new ExclusiveQueueWithBinding("", skc.stormRoutekey);
		Scheme scheme = new RawScheme();
		AMQPSpout spout = new AMQPSpout(skc.stormHost, skc.stormPort, 
						skc.stormUsername, skc.stormPassword, skc.stormVhost, 
						queueDeclaration, scheme);

		//3. create topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", spout, 1);
		builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("spout");
		builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));

		//4. submit topology
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

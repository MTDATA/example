package com.meituan.storm.topology;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.KafkaConfig.ZkHosts;
import storm.kafka.KafkaConfig.StaticHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.google.common.collect.ImmutableList;
import com.meituan.storm.bolt.SplitBolt;
import com.meituan.storm.bolt.WordCountBolt;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology {

    /** Config Brokers information */
    public static String brokersZkServers = "hadoop02:2181,hadoop03:2181,hadoop04:2181";
    public static String brokersZkPath = "/kafka/brokers";

    /** Config used to store kafka offset information **/
	public static ImmutableList<String> zkServers = ImmutableList.of("hadoop02", "hadoop03", "hadoop04");
    public static String zkRoot = "/storm/kafka"; 
	public static String topic = "mobile_push_serviceorg";
	public static Integer topicPartitionNum = 1;
	public static String groupId = "mobile_push_serviceorg_test_group";

	public static void main(String[] args) throws Exception {
		//1. spout
		SpoutConfig spoutConfig = new SpoutConfig(
                new ZkHosts(brokersZkServers, brokersZkPath), 
                topic, zkRoot, groupId);
	    spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = 2181;
		spoutConfig.scheme = new StringScheme(); 
        spoutConfig.forceStartOffsetTime(-2);
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		//2. create topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", kafkaSpout, 1);
		builder.setBolt("split", new SplitBolt(), 1).shuffleGrouping("spout");
		builder.setBolt("count", new WordCountBolt(), 1).fieldsGrouping("split", new Fields("word"));

		//3. submit topology
		Config conf = new Config();
		conf.setDebug(true);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());

			Thread.sleep(60000);

			cluster.shutdown();
		}
	}
}

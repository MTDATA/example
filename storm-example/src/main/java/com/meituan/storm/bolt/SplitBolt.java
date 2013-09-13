package com.meituan.storm.bolt;

import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1999209252187463355L;

	public SplitBolt() {
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String sentence = tuple.getString(0);
      System.out.println("======Bolt recv sentence[" + sentence + "]");
      String[] words = sentence.split(":");
      for (String word : words) {
        collector.emit(new Values(word));
      }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}

package com.meituan.storm.bolt;

import java.util.Map;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class SplitSentenceBolt extends ShellBolt implements IRichBolt {
	private static final long serialVersionUID = 1999209252187463355L;

	public SplitSentenceBolt() {
        super("python", "splitsentence.py");
      }

      public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
      }

      public Map<String, Object> getComponentConfiguration() {
        return null;
      }
}

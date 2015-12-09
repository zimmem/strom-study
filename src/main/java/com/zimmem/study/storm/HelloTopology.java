package com.zimmem.study.storm;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HelloTopology {

    public static class HelloSpout extends BaseRichSpout {

        boolean              isDistributed;
        SpoutOutputCollector collector;

        public HelloSpout() {
            this(true);
        }

        public HelloSpout(boolean isDistributed) {
            this.isDistributed = isDistributed;
        }

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        public void close() {
        }

        public void nextTuple() {
            Utils.sleep(100);
            final String[] words = new String[] { "china", "usa", "japan", "russia", "england" };
            final Random rand = new Random();
            final String word = words[rand.nextInt(words.length)];
            this.collector.emit(new Values(word));
        }

        public void ack(Object msgId) {
        }

        public void fail(Object msgId) {
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            if (!this.isDistributed) {
                Map<String, Object> ret = new TreeMap<String, Object>();
                ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
                return ret;
            } else {
                return null;
            }
        }
    }

    public static class HelloBolt extends BaseRichBolt {

        OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            this.collector.emit(tuple, new Values("hello," + tuple.getString(0)));
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class SplitBolt extends BaseRichBolt {

        OutputCollector collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String string = tuple.getString(0);
            for (int i = 0; i < string.length(); i++) {
                this.collector.emit(tuple, new Values(string.charAt(i)));
            }
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("char"));
        }
    }

    public static class StatBolt extends BaseRichBolt {

        OutputCollector collector;

        Map<Character, Integer> counts = new HashMap<>();

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            char c = (char) tuple.getValue(0);
            if (counts.containsKey(c)) {
                counts.put(c, counts.get(c) + 1);
            } else {
                counts.put(c, 1);
            }
            this.collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // declarer.declare(new Fields("chars"));
        }

        @Override
        public void cleanup() {
            for (Entry<Character, Integer> entry : counts.entrySet()) {
                System.out.println(entry.getKey() + "\t" + entry.getValue());
            }
            super.cleanup();
        }

    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("a", new HelloSpout(), 10);
        builder.setBolt("b", new HelloBolt(), 5).shuffleGrouping("a");
        builder.setBolt("c", new SplitBolt(), 2).shuffleGrouping("b");
        //builder.setBolt("d", new StatBolt(), 10).shuffleGrouping("c");
        builder.setBolt("d", new StatBolt(), 10).fieldsGrouping("c", new Fields("char"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            String test_id = "hello_test";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(test_id, conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology(test_id);
            cluster.shutdown();
        }
    }
}

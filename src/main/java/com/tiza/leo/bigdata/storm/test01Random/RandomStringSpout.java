package com.tiza.leo.bigdata.storm.test01Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author leowei
 * @date 2021/4/8  - 21:44
 */
public class RandomStringSpout extends BaseRichSpout {

    Map<Integer,String> map =new HashMap<>();
    SpoutOutputCollector collector;

    //
    public RandomStringSpout(){
        map.put(0, "kafka Stream");
        map.put(1, "Storm");
        map.put(2, "Spark");
        map.put(3, "Flink");
        map.put(3, ".....");
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector =collector;
    }

    //不断的获取tuple 将 tuple 发送给 stream
    @Override
    public void nextTuple() {
        collector.emit(new Values(map.get(ThreadLocalRandom.current().nextInt(0, 5))));

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("streamSpout"));
    }
}

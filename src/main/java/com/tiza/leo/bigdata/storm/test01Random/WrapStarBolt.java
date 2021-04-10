package com.tiza.leo.bigdata.storm.test01Random;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;


/**
 * @author leowei
 * @date 2021/4/8  - 22:08
 */
public class WrapStarBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String streamSpout = tuple.getStringByField("streamSpout");
        System.out.println("******" +streamSpout);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

package com.tiza.leo.bigdata.storm.test02Parallel;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.lang.Thread.currentThread;

/**
 * @author leowei
 * @date 2021/4/10  - 10:14
 */
public class SimpleBolt extends BaseBasicBolt {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleBolt.class);
    TopologyContext context;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        this.context = context;
        LOGGER.warn("SimpleBolt -> prepare: hashcode{}->thread:{},taskId:{}",this.hashCode(),currentThread(),context.getThisTaskId());
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Integer i = tuple.getIntegerByField("i");
        LOGGER.warn("SimpleBolt -> execute: hashcode{}->thread:{},taskId:{},value i is :{}",this.hashCode(),currentThread(),context.getThisTaskId(),i);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //无需发送，所以无需操作
    }

    @Override
    public void cleanup() {
        super.cleanup();
        LOGGER.warn("SimpleBolt -> cleanup: hashcode{}->thread:{},taskId:{}",this.hashCode(),currentThread(),context.getThisTaskId());
    }
}

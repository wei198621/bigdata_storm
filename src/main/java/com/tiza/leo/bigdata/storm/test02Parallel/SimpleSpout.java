package com.tiza.leo.bigdata.storm.test02Parallel;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.currentThread;

/**
 * @author leowei
 * @date 2021/4/10  - 10:01
 */
public class SimpleSpout extends BaseRichSpout {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleSpout.class);
    TopologyContext context;
    SpoutOutputCollector collector;
    private AtomicInteger  i;
    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {

        this.context=context;
        this.collector = collector;
        this.i = new AtomicInteger();
        LOGGER.warn("SimpleSpout -> open: hashcode{}->thread:{},taskId:{}",this.hashCode(),currentThread(),context.getThisTaskId());
    }

    @Override
    public void nextTuple() {
        int i = this.i.incrementAndGet();
        if(i<=10)
        {

            LOGGER.warn("SimpleSpout -> nextTuple: hashcode{}->thread:{},taskId:{},Value i is :{}",this.hashCode(),currentThread(),context.getThisTaskId(),i);
            collector.emit(new Values(i));
        }
        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            //e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("i"));
    }

    @Override
    public void close() {
        super.close();
        LOGGER.warn("SimpleSpout -> close: hashcode{}->thread:{},taskId:{}",this.hashCode(),currentThread(),context.getThisTaskId());

    }
}

package com.tiza.leo.bigdata.storm.test01Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author leowei
 * @date 2021/4/8  - 22:16
 */
public class RandomStringTopologyLocal {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RandomStringSpout", new RandomStringSpout());
        builder.setBolt("WrapStarBolt", new WrapStarBolt()).shuffleGrouping("RandomStringSpout");
        builder.setBolt("WrapWellBolt", new WrapWellBolt()).shuffleGrouping("RandomStringSpout");

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("RandomStringTopologyLocal",config,builder.createTopology());

        System.out.println("the first topology start running ....");

        System.out.println("...............");
        cluster.killTopology("RandomStringTopologyLocal");
        cluster.shutdown();
    }
}

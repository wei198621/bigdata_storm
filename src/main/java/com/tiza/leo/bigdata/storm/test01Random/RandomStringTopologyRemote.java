package com.tiza.leo.bigdata.storm.test01Random;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author leowei
 * @date 2021/4/8  - 22:59
 */
public class RandomStringTopologyRemote {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("RandomStringSpout", new RandomStringSpout());
        // 01  设置并行 个数
        builder.setBolt("WrapStarBolt", new WrapStarBolt(), 4).shuffleGrouping("RandomStringSpout");
        builder.setBolt("WrapWellBolt", new WrapWellBolt(), 4).shuffleGrouping("RandomStringSpout");

        //02  设置工作进程个数
        Config conf = new Config();
        conf.setNumWorkers(3);

        try {
            // 03 提交方式与本地模式不同
            StormSubmitter.submitTopology("RandomStringTopologyRemote",conf,builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException  e) {
            e.printStackTrace();
        }

    }
}

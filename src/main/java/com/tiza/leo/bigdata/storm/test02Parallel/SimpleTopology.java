package com.tiza.leo.bigdata.storm.test02Parallel;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author leowei
 * @date 2021/4/10  - 10:22
 */
public class SimpleTopology {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleTopology.class);

    /** topologyname
     * component profix
     * workers
     * spout executor size (parallel hint)
     * spout task size
     * bolt executor size (parallel hint)
     * bolt task size
     *
     * @param args
     */
    public static void main(String[] args) {
        if(args.length<7){
            throw new IllegalArgumentException(" the args is Illegal ....");
        }
        Options opts = Options.build(args);
        LOGGER.warn("Topology-Options:{}",opts);

        final TopologyBuilder builder = new TopologyBuilder();
        String spoutName = opts.getPrefix() + "-SimpleSpout";
        String boltName = opts.getPrefix() + "-SimpleBolt";
        // builder.setSpout(spoutName,new SimpleSpout(),opts.getSpoutParallelHint())  不写NumTasks的时候 NumTasks数量与 ParallelHint一致
        builder.setSpout(spoutName,new SimpleSpout(),opts.getSpoutParallelHint()).setNumTasks(opts.getBoltTasks());
        builder.setBolt(boltName, new SimpleBolt(), opts.getBoltParallelHint()).setNumTasks(opts.getBoltTasks())
                .shuffleGrouping(spoutName);

        final Config config = new Config();
        config.setNumWorkers(opts.getWorkers());

        try {
            StormSubmitter.submitTopology(opts.getTopologyName(),config,builder.createTopology());
            LOGGER.warn("=========================================================");
            LOGGER.warn(" THE topology {} is submited ",opts.getTopologyName());
            LOGGER.warn("=========================================================");
        } catch (AlreadyAliveException |InvalidTopologyException |AuthorizationException e) {
            e.printStackTrace();
        }
    }

    private static class Options{
        private final String topologyName;
        private final String prefix;
        private final int workers;
        private final int spoutParallelHint;
        private final int spoutTasks;
        private final int boltParallelHint;
        private final int boltTasks;

        private Options(String topologyName, String prefix, int workers,
                       int spoutParallelHint, int spoutTasks, int boltParallelHint, int boltTasks) {
            this.topologyName = topologyName;
            this.prefix = prefix;
            this.workers = workers;
            this.spoutParallelHint = spoutParallelHint;
            this.spoutTasks = spoutTasks;
            this.boltParallelHint = boltParallelHint;
            this.boltTasks = boltTasks;
        }

        //构造函数供 外部用户调用
        static Options build(String[] args){
            return new Options(args[0],args[1],Integer.parseInt(args[2]),
                    Integer.parseInt(args[3]),Integer.parseInt(args[4]),
                    Integer.parseInt(args[5]),Integer.parseInt(args[6]));
        }

         String getTopologyName() {
            return topologyName;
        }

         String getPrefix() {
            return prefix;
        }

         int getWorkers() {
            return workers;
        }

         int getSpoutParallelHint() {
            return spoutParallelHint;
        }

         int getSpoutTasks() {
            return spoutTasks;
        }

         int getBoltParallelHint() {
            return boltParallelHint;
        }

         int getBoltTasks() {
            return boltTasks;
        }

        @Override
        public String toString() {
            return "Options{" +
                    "topologyName='" + topologyName + '\'' +
                    ", prefix='" + prefix + '\'' +
                    ", workers=" + workers +
                    ", spoutParallelHint=" + spoutParallelHint +
                    ", spoutTasks=" + spoutTasks +
                    ", boltParallelHint=" + boltParallelHint +
                    ", boltTasks=" + boltTasks +
                    '}';
        }
    }
}

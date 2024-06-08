package com.jyong.flink.job;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author jyong
 * @Date 2023/5/20 12:38
 * @desc
 */

public class CheckpointConfigExample {


    public static void main(String[] args) {

        //1.创建执行坏境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.设置并行度
        env.setParallelism(1);


        //检查见配置

        //1.检查点保存时间间隔：每隔1m启动一次检查点保存
        env.enableCheckpointing(1000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        /**
         * 检查点保存的超时时间
         * 超时时间用于指定检查点保存的超时时间，超时没有完成就被丢弃。传入一个长整型毫秒数作为参数，表示超时时间。
         */
        checkpointConfig.setCheckpointTimeout(60000L);

        /**
         * 检查点模式
         * EXACTLY_ONCE ： 精确一次
         * AT_LEAST_ONCE : 至少一次
         */
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        /**
         * 最小间隔时间
         * 最小间隔时间用于指定在上一次检查点完成之前，捡柴旦协调器做快等多久可以发出保存下一个检查点的指令。
         * 这就意味着即使已经达到了周期触发的时间点，只要距离上一个检查点完成的间隔时间间隔不够，就依然不能开启下一次检查点的保存。
         * 这就为正常处理数据留下了充足的间隙。当指定这个参数时，最大并发检查点数量【maxConcurrentCheckpoints】的值强制为1。
         *
         */
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
        /**
         * 最大检查点数量
         * 最大并发检查点数量用于指定运行中的检查点最多可以有多少个。由于每个任务的处理进度不同，所以完全可能后面的任务还没有完成完成前一个检查点的保存，
         * 而前面的任务已经开始保存下一个检查点的清空。这个参数就是用来限制同时进行的检查点的最大数量的
         */
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        /**
         * 弃用参数 开启外部持久化存储
         * 开启外部持久化存储用于开启检查点的外部持久化，而且默认在作业失败的时候不会清理，如果想释放空间，则需要自己手动清理。
         * 里面传入的参数ExternalizedCheckpointCleanup指定了作业取消时外部检查点该如何清理
         * DELETE_ON_CANCELLATION : 在作业取消掉时候会自动删除外部检查点，但是如果由于作业失败而退出，则会保留检查点
         * NO_EXTERNALIZED_CHECKPOINTS ：在作业取消的时候会保留外部检查点。
         */
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS);

        /**
         * 检查点失败次数容忍值
         */
        checkpointConfig.setTolerableCheckpointFailureNumber(0);
        /**
         * 不对齐检查点
         * 不对齐检查点是指不再执行检查点的分界点对其操作，启用之后可以大大缩短产生背压时的检查点保存时间。
         * 这个设置要求检查点模式（CheckpointingMode）为EXACTLY_ONCE精确一次，并且并发检查点个数为1个
         */
        checkpointConfig.enableUnalignedCheckpoints();

    }


}

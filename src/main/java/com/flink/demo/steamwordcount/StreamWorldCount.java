package com.flink.demo.steamwordcount;

import com.flink.demo.workcount.WokCount;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author dengff
 * @date 2021/8/18-23:10
 **/
// 流处理
public class StreamWorldCount {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置cpu核心数
        //senv.setParallelism(4);
        // 从文件中读取数据
        String path = "E:\\project\\flink-demo\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputDataStreamSource = senv.readTextFile(path);

        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = inputDataStreamSource.flatMap(new WokCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        resultStream.print();

        // 执行任务
        senv.execute();
    }

}

package com.flink.demo.workcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author dengff
 * @date 2021/8/18-22:47
 **/
// 批处理 word count
public class WokCount {

    public static void main(String[] args) throws Exception {
        // 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String path = "E:\\project\\flink-demo\\src\\main\\resources\\hello.txt";
        DataSource<String> inputDataSource = env.readTextFile(path);

        // 对数据集进行处理，按空格分词展开，转换成（word,1）进行统计
        DataSet<Tuple2<String, Integer>> sum = inputDataSource.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照第一个位置的word分组
                .sum(1);// 将第二个位置上的数据求和

        sum.print();
    }

    // 自定义类，实现FaltFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

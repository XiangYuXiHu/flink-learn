package com.smile.stream;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * 从文件中读取
 *
 * @Description
 * @ClassName FileStream
 * @Author smile
 * @date 2022.08.07 20:06
 */
public class FileStream {

    /**
     * environment.readTextFile(path).print();
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        readFile(environment);
    }

    public static void readFile(StreamExecutionEnvironment env) throws Exception {
        String path = "src/main/resources/hello.txt";
        env.readFile(new TextInputFormat(new Path(path)),
                path, FileProcessingMode.PROCESS_ONCE, 1, BasicTypeInfo.STRING_TYPE_INFO)
                .print();

        env.execute("read file");
    }
}

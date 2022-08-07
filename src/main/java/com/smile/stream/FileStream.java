package com.smile.stream;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
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
        String path = "d:/flink/data/hello.txt";
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.readFile(new TextInputFormat(new Path(path)),
                path, FileProcessingMode.PROCESS_ONCE,
                1, BasicTypeInfo.STRING_TYPE_INFO).print();
        environment.execute("file stream");
    }
}

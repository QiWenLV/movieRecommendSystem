package com.zqw.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {

    public static final String PREFIX_MSG = "abc:";

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    //用于处理数据
    @Override
    public void process(byte[] key, byte[] value) {
        String ratingValue = new String(value);
        //判断是否是需要的埋点日志
        if(ratingValue.contains(PREFIX_MSG)){
            String bValue = ratingValue.split(PREFIX_MSG)[1];
            context.forward("log".getBytes(), bValue.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}

package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    //前缀后缀
    private String prefix;
    private String subfix;
    private Long delay;

    //最先走配置文件
    @Override
    public void configure(Context context) {
        prefix = context.getString("pre", "pre-");
        subfix = context.getString("sub");
        delay = context.getLong("delay",2000L);
    }

    @Override
    public Status process() throws EventDeliveryException {

        try {
            //声明事件
            Event event = new SimpleEvent();
            HashMap<String, String> header = new HashMap<>();
            //循环创建事件信息，传给channel
            for (int i = 0; i < 5; i++) {
                event.setHeaders(header);
                event.setBody((prefix+"atguigu:"+i+subfix).getBytes());
                getChannelProcessor().processEvent(event);
            }
            Thread.sleep(delay);
            return Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }

    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}

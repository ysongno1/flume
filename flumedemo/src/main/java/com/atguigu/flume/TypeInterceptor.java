package com.atguigu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    //声明一个集合用来存储拦截器后处理的事件
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        //初始化集合用来存储拦截器后处理的事件
        addHeaderEvents = new ArrayList<>();
    }

    //单个事件处理方法
    @Override
    public Event intercept(Event event) {

        //获取header&body
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());

        //根据body中是否包含"atguigu"添加不同的头部信息
        if (body.contains("atguigu"))
        {
            headers.put("type","atguigu");
        } else {
            headers.put("type", "other");
        }

        //返回数据
        return event;
    }

    //批量数据处理方式
    @Override
    public List<Event> intercept(List<Event> events) {

        //1.清空集合（不清空的话上个批次等等会继续存在集合中 造成重复上传）
        addHeaderEvents.clear();

        //2.遍历events
        for (Event event : events) {
            addHeaderEvents.add(intercept(event));
        }

        //返回数据
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    //利用静态内部类构建(new)拦截器
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        //读取配置文件
        @Override
        public void configure(Context context) {

        }
    }
}

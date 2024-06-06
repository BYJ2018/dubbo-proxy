package com.mds.dubbo.session;

import com.mds.dubbo.config.AppInfo;
import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 链接管理器
 *
 * @author baoyoujia
 * @date 2024/5/27
 */
public class ConnectionManager {

    /**
     * consumer channel和provider信息
     */
    private final Map<Channel, List<ConnectionInfo>> connectionRegistry =
            new ConcurrentHashMap<>();



    private final Map<String,Channel> readyMap = new ConcurrentHashMap<>(16);

    /**
     * 重试
     */
    private final Map<Channel, List<AppInfo>> retryMap = new ConcurrentHashMap<>();


    private ConnectionManager() {

    }

    public Channel getConnection(String appName) {
        return readyMap.get(appName);
    }

    private static class Singleton {
        static ConnectionManager instance = new ConnectionManager();
    }

    public static ConnectionManager getInstance() {
        return ConnectionManager.Singleton.instance;
    }

    public void addConnection(String appName, Channel channel) {
        readyMap.put(appName, channel);
    }

    public Collection<Channel> connection() {
        return readyMap.values();
    }

    public void retryQueue(Channel channel, List<AppInfo> appInfoList) {
        retryMap.put(channel, appInfoList);
    }

    public List<AppInfo> getQueue(Channel channel) {
        return retryMap.get(channel);
    }

    public boolean exist(Channel channel) {
        return connectionRegistry.containsKey(channel);
    }

}

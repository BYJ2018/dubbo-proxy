package com.mds.dubbo.session;

import com.mds.dubbo.config.AppInfo;
import io.netty.channel.Channel;

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

    /**
     * 重试
     */
    private final Map<Channel, List<AppInfo>> retryMap = new ConcurrentHashMap<>();


    private ConnectionManager() {

    }

    private static class Singleton {
        static ConnectionManager instance = new ConnectionManager();
    }

    public static ConnectionManager getInstance() {
        return ConnectionManager.Singleton.instance;
    }

    public void addConnection(Channel inboundChannel, List<ConnectionInfo> connectionInfoList) {
        connectionRegistry.put(inboundChannel, connectionInfoList);
    }

    public List<ConnectionInfo> getConnectionInfo(Channel inboundChannel) {
        return connectionRegistry.get(inboundChannel);
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

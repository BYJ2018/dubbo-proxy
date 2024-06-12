package com.mds.dubbo.session;

import com.mds.dubbo.config.AppInfo;
import com.sun.jmx.remote.internal.ArrayQueue;
import io.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 链接管理器
 *
 * @author baoyoujia
 * @date 2024/5/27
 */
public class ConnectionManager {

    /**
     * 维护 客户端的inboundChannel 和各个 后端真实服务间的channel 映射
     */
    private final Map<Channel, List<Channel>> clientToServerChannelMap = new ConcurrentHashMap<>(16);

    /**
     * 维护各个真实服务与客户端channel的映射
     */
    private final Map<Channel, Channel> serverToClientChannelMap = new ConcurrentHashMap<>(16);

    /**
     * 维护服务端channel 和 真实服务间的链接信息
     */
    private final Map<Channel, AppInfo> channelAppInfoMap = new ConcurrentHashMap<>(16);

    private final Map<String,Channel> readyMap = new ConcurrentHashMap<>(16);

    /**
     * 重试
     */
    private final Queue<Channel> retryMap = new LinkedBlockingQueue<>(100);


    private ConnectionManager() {

    }

    public Channel getConnection(String appName) {
        return readyMap.get(appName);
    }

    /**
     * 如果client与proxy的链接已经断开了， 需要将与后端真实服务建立的关系移除，链接关闭
     * @param channel 入栈通道
     */
    public void removeChannel(Channel channel) {
        clientToServerChannelMap.get(channel).forEach(c -> {
            AppInfo appInfo = channelAppInfoMap.get(c);
            readyMap.remove(appInfo.getName());
            channelAppInfoMap.remove(c);
            c.close();
        });
        serverToClientChannelMap.entrySet().removeIf(channelEntry -> channelEntry.getValue() == channel);
        clientToServerChannelMap.remove(channel);
    }

    private static class Singleton {
        static ConnectionManager instance = new ConnectionManager();
    }

    public static ConnectionManager getInstance() {
        return ConnectionManager.Singleton.instance;
    }

    public void addConnection(AppInfo appInfo, Channel channel, Channel inboundChannel) {
        // 插入 client channel 和 server channel
        clientToServerChannelMap.computeIfAbsent(inboundChannel, k -> new ArrayList<>()).add(channel);
        // 插入 server channel 对应的 client channel
        serverToClientChannelMap.put(channel, inboundChannel);
        readyMap.put(appInfo.getName(), channel);
        channelAppInfoMap.put(channel, appInfo);
    }

    public AppInfo getAppInfo(Channel channel) {
        return channelAppInfoMap.get(channel);
    }

    public Collection<Channel> connection() {
        return readyMap.values();
    }

    public Channel getInboundChannel(Channel channel) {
        return serverToClientChannelMap.get(channel);
    }

    public void retryQueue(Channel channel) {
        retryMap.offer(channel);
    }

    public Channel poll() {
        Channel channel = retryMap.poll();
        if (channel != null) {
            channel.close();
        }
        return channel;
    }

}

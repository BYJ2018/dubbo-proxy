package com.mds.dubbo.session;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * session管理器
 *
 * @author baoyoujia
 * @date 2024/5/23
 */
@Slf4j
public class SessionManager {

    private SessionManager() {

    }

    public void renew(Channel channel) {
        log.info("channelId：{}, renew: {}", channel, System.currentTimeMillis() + 180000);
        putSession(channel, System.currentTimeMillis() + 180000);
    }

    private static class Singleton {
        static SessionManager instance = new SessionManager();
    }

    public static SessionManager getInstance() {
        return Singleton.instance;
    }

    private final Map<Channel, Long> sessionRegistry =
            new ConcurrentHashMap<>();

    public void putSession(Channel channel) {
        log.debug("新的客户端加入：{}", channel);
        sessionRegistry.put(channel, System.currentTimeMillis() + 180000);
    }

    public void putSession(Channel channel, Long expireTime) {
        sessionRegistry.put(channel, expireTime);
    }

    public void removeSession(Channel channel) {
        sessionRegistry.remove(channel);
    }

    public Long getSession(Channel channel) {
        return sessionRegistry.get(channel);
    }

    public boolean exist(Channel channel) {
        return sessionRegistry.containsKey(channel);
    }
}

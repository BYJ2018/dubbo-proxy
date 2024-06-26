package com.mds.dubbo.handler;

import com.mds.dubbo.session.ConnectionManager;
import com.mds.dubbo.session.SessionManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * session处理器
 *
 * @author baoyoujia
 * @date 2024/5/23
 */
@Slf4j
public class SessionHandler extends ChannelInboundHandlerAdapter {

    private final HashedWheelTimer timer = new HashedWheelTimer();

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        log.info("SessionHandler channel is active: {}", ctx.channel().remoteAddress());
        Channel channel = ctx.channel();
        SessionManager sessionManager = SessionManager.getInstance();
        // 180秒
        sessionManager.putSession(channel);
        scheduleTimeoutCheck(channel);
        ctx.fireChannelActive();
    }

    /**
     * 超时检查
     * @param channel
     */
    private void scheduleTimeoutCheck(Channel channel) {
        SessionManager sessionManager = SessionManager.getInstance();
        timer.newTimeout(timeout -> {
            if (!sessionManager.exist(channel)) {
                return;
            }
            Long expireTime = sessionManager.getSession(channel);
            long currentTime = System.currentTimeMillis();
            log.warn("channel:{}, current time : {}, expireTime : {}", channel, currentTime, expireTime);
            if (currentTime - expireTime > TimeUnit.SECONDS.toMillis(30)) {
                // 已经超时，准备关闭
                log.warn("已经超时，准备关闭");
                // 超时，关闭Channel
                channel.close();
                sessionManager.removeSession(channel);
            } else {
                // 重新启动定时
                // 重新安排定时检查
                scheduleTimeoutCheck(channel);
            }
        }, 10, TimeUnit.SECONDS);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("SessionHandler channelRead  msg {}",msg);
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        log.info("SessionHandler channel is inActive: {}, id：{}", channel.remoteAddress(), channel.id().asLongText());
        SessionManager sessionManager = SessionManager.getInstance();
        sessionManager.removeSession(channel);
        ConnectionManager connectionManager = ConnectionManager.getInstance();
        connectionManager.removeChannel(channel);
        timer.stop();
    }
}

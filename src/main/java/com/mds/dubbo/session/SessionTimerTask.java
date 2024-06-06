package com.mds.dubbo.session;

import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * @author baoyoujia
 * @date 2024/5/30
 */
@Slf4j
public class SessionTimerTask implements TimerTask {

    private HashedWheelTimer hashedWheelTimer;
    private Channel channel;

    public SessionTimerTask(HashedWheelTimer hashedWheelTimer, Channel channel) {
        this.hashedWheelTimer = hashedWheelTimer;
        this.channel = channel;
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        SessionManager sessionManager = SessionManager.getInstance();
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
        }
        hashedWheelTimer.newTimeout(this, 10, TimeUnit.SECONDS);
    }
}

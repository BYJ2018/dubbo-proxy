package com.mds.dubbo.handler;

import com.mds.dubbo.codec.DubboRequestDecoder;
import com.mds.dubbo.config.AppInfo;
import com.mds.dubbo.session.ConnectionManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.HashedWheelTimer;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.remoting.transport.netty4.NettyEventLoopFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

import static com.mds.dubbo.codec.Constant.*;
import static com.mds.dubbo.core.Serialization.getNullBytesOf;

/**
 * @author baoyoujia
 * @date 2024/6/5
 */
@Slf4j
public class ConnectionHandler  extends ChannelInboundHandlerAdapter {

    private final List<AppInfo> appInfoList;

    private final List<AppInfo> retryList = new CopyOnWriteArrayList<>();

    public ConnectionHandler(List<AppInfo> appInfoList) {
        this.appInfoList = appInfoList;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel inboundChannel = ctx.channel();
        // 与真实服务建立连接
        appInfoList.forEach(appInfo -> connect(appInfo, inboundChannel));
        // 与真实服务的心跳
        heartBeat();
        // 与真实服务重试，建立链接
        retry();
        ctx.fireChannelActive();
    }

    /**
     * 重试队列
     */
    private void retry() {
        new Thread(() -> {
            ConnectionManager connectionManager = ConnectionManager.getInstance();
            while (true) {
                Channel channel = connectionManager.poll();
                if (channel != null) {
                    Channel inboundChannel = connectionManager.getInboundChannel(channel);
                    AppInfo appInfo = connectionManager.getAppInfo(channel);
                    connect(appInfo, inboundChannel);
                } else {
                    try {
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }).start();

    }

    private void heartBeat() {
        ConnectionManager connectionManager = ConnectionManager.getInstance();
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            for (Channel channel : connectionManager.connection()) {
                ByteBuf buffer = heartbeatPacket();
                if (channel.isActive()) {
                    log.warn("send heartbeat: {}", channel);
                    channel.writeAndFlush(buffer);
                } else {
                    // 加入重试队列
                    connectionManager.retryQueue(channel);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("ConnectionHandler channelRead  msg {}",msg);
        ctx.fireChannelRead(msg);
    }

    private ByteBuf heartbeatPacket() {
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.buffer(17);
        buffer.writeShort(MAGIC);
        buffer.writeByte(FLAG_REQUEST | FLAG_TWOWAY | FLAG_EVENT | SERIALIZATION_ID);
        buffer.writeByte((byte) 0x00);
        buffer.writeLong(System.currentTimeMillis());
        buffer.writeInt(1);
        byte[] nullBytesOf = getNullBytesOf((byte) 2);
        buffer.writeBytes(nullBytesOf);
        return buffer;
    }

    private void connect(AppInfo appInfo, Channel inboundChannel) {
        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop());
        b.option(ChannelOption.AUTO_READ, true)
                .channel(NettyEventLoopFactory.socketChannelClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new BackendHandler());
                    }
                });
        ChannelFuture channelFuture = b.connect(appInfo.getIp(), appInfo.getPort());
        channelFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.info("remote {} connect success", inboundChannel.remoteAddress());
                inboundChannel.read();
                Channel channel = channelFuture.channel();
                ConnectionManager connectionManager = ConnectionManager.getInstance();
                connectionManager.addConnection(appInfo, channel, inboundChannel);
            } else {
                // 如果直接连接失败了，一直进行重试
                future.channel().eventLoop().schedule(() -> connect(appInfo, inboundChannel), 5, TimeUnit.SECONDS);
            }
        });
    }
}

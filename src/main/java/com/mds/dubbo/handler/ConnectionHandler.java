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

    private final HashedWheelTimer timer = new HashedWheelTimer();

    private final Map<Channel, AppInfo> map = new ConcurrentHashMap<>(16);

    private final List<AppInfo> retryList = new CopyOnWriteArrayList<>();

    public ConnectionHandler(List<AppInfo> appInfoList) {
        this.appInfoList = appInfoList;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel inboundChannel = ctx.channel();
        for (AppInfo appInfo : appInfoList) {
            connect(appInfo, inboundChannel);
        }
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            ConnectionManager connectionManager = ConnectionManager.getInstance();
            for (Channel channel : connectionManager.connection()) {
                ByteBuf buffer = heartbeatPacket();
                if (channel.isActive()) {
                    log.warn("send heartbeat: {}", channel);
                    channel.writeAndFlush(buffer);
                } else {
                    // 重试队列
                    AppInfo appInfo = map.get(channel);
                    if (Objects.isNull(appInfo)) {
                        break;
                    }
                    connect(appInfo, inboundChannel);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
        ctx.fireChannelActive();
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
                        socketChannel.pipeline().addLast(new DubboRequestDecoder());
                        socketChannel.pipeline().addLast(new BackendHandler(inboundChannel));
                    }
                });
        ChannelFuture channelFuture = b.connect(appInfo.getIp(), appInfo.getPort());
        channelFuture.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.info("remote {} connect success", inboundChannel.remoteAddress());
                inboundChannel.read();
                ConnectionManager connectionManager = ConnectionManager.getInstance();
                connectionManager.addConnection(appInfo.getName(),channelFuture.channel());
            } else {
                // 暂存,定时重连重连。
                log.warn("connect fail {}", appInfo);
                /*inboundChannel.close();*/
                retryList.add(appInfo);
            }
        });
    }
}

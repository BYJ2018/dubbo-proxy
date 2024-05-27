package com.mds.dubbo.handler;
import com.mds.dubbo.codec.packet.Body;
import com.mds.dubbo.codec.packet.BodyHeartBeat;
import com.mds.dubbo.codec.packet.BodyRequest;
import com.mds.dubbo.codec.packet.DubboPacket;
import com.mds.dubbo.config.AppInfo;
import com.mds.dubbo.session.ConnectionInfo;
import com.mds.dubbo.session.ConnectionManager;
import com.mds.dubbo.session.SessionManager;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.util.HashedWheelTimer;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.remoting.transport.netty4.NettyEventLoopFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.mds.dubbo.codec.Constant.*;
import static com.mds.dubbo.core.Serialization.getNullBytesOf;

@Slf4j
public class FrontendHandler extends ChannelInboundHandlerAdapter {

    private final List<AppInfo> appInfoList;

    private final HashedWheelTimer timer = new HashedWheelTimer();

    public FrontendHandler(List<AppInfo> appInfoList) {
        this.appInfoList = appInfoList;
    }

    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Channel inboundChannel = ctx.channel();
        List<ConnectionInfo> connectionInfoList = new ArrayList<>(appInfoList.size());
        List<AppInfo> retryList = new ArrayList<>(appInfoList.size());
        for (AppInfo appInfo : appInfoList) {
            ChannelFuture channelFuture = connect(appInfo, inboundChannel);
            channelFuture.addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    log.info("remote {} connect success", inboundChannel.remoteAddress());
                    inboundChannel.read();
                    ConnectionInfo connectionInfo = new ConnectionInfo();
                    connectionInfo.setChannel(future.channel());
                    connectionInfo.setAppInfo(appInfo);
                    connectionInfoList.add(connectionInfo);
                } else {
                    // 暂存,定时重连重连。
                    log.warn("connect fail {}", appInfo);
                    /*inboundChannel.close();*/
                    retryList.add(appInfo);
                }
            });
        }
        ConnectionManager connectionManager = ConnectionManager.getInstance();
        connectionManager.addConnection(inboundChannel, connectionInfoList);
        connectionManager.retryQueue(inboundChannel, retryList);
        heartbeat(inboundChannel);
    }

    private void heartbeat(Channel inboundChannel) {
        ConnectionManager connectionManager = ConnectionManager.getInstance();
        timer.newTimeout(timeout -> {
            if (!connectionManager.exist(inboundChannel)) {
                return;
            }
            List<ConnectionInfo> connectionInfoList = connectionManager.getConnectionInfo(inboundChannel);
            for (ConnectionInfo connectionInfo : connectionInfoList) {
                Channel channel = connectionInfo.getChannel();
                if (channel.isActive()) {
                    ByteBuf buffer = heartbeatPacket();
                    log.warn("send heartbeat: {}", channel);
                    channel.writeAndFlush(buffer);
                } else {
                    log.warn("重试");
                }
            }
            heartbeat(inboundChannel);
        }, 10, TimeUnit.SECONDS);
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

    private ChannelFuture connect(AppInfo appInfo, Channel inboundChannel) {
        Bootstrap b = new Bootstrap();
        b.group(inboundChannel.eventLoop());
        b.option(ChannelOption.AUTO_READ, true)
                .channel(NettyEventLoopFactory.socketChannelClass())
                .handler(new BackendHandler(inboundChannel));
        return b.connect(appInfo.getIp(), appInfo.getPort());
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        log.info("channelRead  msg {}",msg);
        if (msg instanceof DubboPacket) {
            DubboPacket dubboPacket = (DubboPacket) msg;
            try {
                Body body = dubboPacket.getBody();
                if (body instanceof BodyRequest) {
                    BodyRequest bodyRequest = (BodyRequest) body;
                    String dubboApplication = bodyRequest.getAttachments().get("target-application").toString();
                    ConnectionManager connectionManager = ConnectionManager.getInstance();
                    List<ConnectionInfo> connectionInfo = connectionManager.getConnectionInfo(ctx.channel());
                    Channel channel = connectionInfo.stream().filter(x -> x.getAppInfo().getName().equals(dubboApplication)).map(ConnectionInfo::getChannel).findFirst().orElse(null);
                    if (channel.isActive()) {
                        // 获取ByteBufAllocator用于创建新的ByteBuf
                        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
                        compositeByteBuf.addComponent(true, dubboPacket.getDubboRequestHeader().getHeaderBytes());
                        compositeByteBuf.addComponent(true, dubboPacket.getBody().bytes());
                        channel.writeAndFlush(compositeByteBuf).addListener((ChannelFutureListener) future -> {
                            if (future.isSuccess()) {
                                ctx.channel().read();
                            } else {
                                future.channel().close();
                            }
                        });
                    }
                } else if (body instanceof BodyHeartBeat) {
                    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
                    compositeByteBuf.addComponent(true, dubboPacket.getDubboRequestHeader().getHeaderBytes());
                    log.info("心跳包");
                    SessionManager sessionManager = SessionManager.getInstance();
                    Channel channel = ctx.channel();
                    sessionManager.renew(channel);
                    ctx.writeAndFlush(compositeByteBuf).addListener((ChannelFutureListener) future -> {
                        if (future.isSuccess()) {
                            log.info("success");
                            ctx.channel().read();
                        } else {
                            log.info("fail");
                            future.channel().close();
                        }
                    });;
                }
            } finally {
                dubboPacket.release();
            }

        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.warn("channelInactive: {}", ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }
}

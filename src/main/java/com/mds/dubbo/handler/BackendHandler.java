package com.mds.dubbo.handler;

import com.mds.dubbo.codec.packet.Body;
import com.mds.dubbo.codec.packet.BodyHeartBeat;
import com.mds.dubbo.codec.packet.BodyRequest;
import com.mds.dubbo.codec.packet.DubboPacket;
import com.mds.dubbo.session.ConnectionInfo;
import com.mds.dubbo.session.ConnectionManager;
import com.mds.dubbo.session.SessionManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.mds.dubbo.codec.Constant.FLAG_EVENT;
import static com.mds.dubbo.codec.Constant.OK;


@Slf4j
public class BackendHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        log.info("BackendHandler channelRead  msg {}",msg);
        if (msg instanceof ByteBuf) {
            ByteBuf byteBuf = (ByteBuf) msg;
            byte flag = byteBuf.getByte(2);
            byte status = byteBuf.getByte(3);
            if (status == OK) {
                // 这里的判断不太严谨，需要根据序列化的方式然后判断第17位是不是null result
                if ((flag & FLAG_EVENT) != 0) {
                    log.warn("接收到provider的心跳响应{}", msg);
                } else {
                    ConnectionManager connectionManager = ConnectionManager.getInstance();
                    Channel inboundChannel = connectionManager.getInboundChannel(ctx.channel());
                    inboundChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                        if (future.isSuccess()) {
                            ctx.channel().read();
                        } else {
                            future.channel().close();
                        }
                    });
                }
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ConnectionManager connectionManager = ConnectionManager.getInstance();
        Channel inboundChannel = connectionManager.getInboundChannel(ctx.channel());
        FrontendHandler.closeOnFlush(inboundChannel);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        FrontendHandler.closeOnFlush(ctx.channel());
    }
}

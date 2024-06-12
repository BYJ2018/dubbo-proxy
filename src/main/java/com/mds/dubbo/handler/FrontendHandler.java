package com.mds.dubbo.handler;

import com.mds.dubbo.codec.packet.Body;
import com.mds.dubbo.codec.packet.BodyHeartBeat;
import com.mds.dubbo.codec.packet.BodyRequest;
import com.mds.dubbo.codec.packet.DubboPacket;
import com.mds.dubbo.session.ConnectionManager;
import com.mds.dubbo.session.SessionManager;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FrontendHandler extends ChannelInboundHandlerAdapter {

    public FrontendHandler() {
    }

    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("SessionHandler channel is active: {}", ctx.channel().remoteAddress());
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        log.info("FrontendHandler channelRead  msg {}",msg);
        if (msg instanceof DubboPacket) {
            DubboPacket dubboPacket = (DubboPacket) msg;
            try {
                Body body = dubboPacket.getBody();
                if (body instanceof BodyRequest) {
                    BodyRequest bodyRequest = (BodyRequest) body;
                    String dubboApplication = bodyRequest.getAttachments().get("target-application").toString();
                    ConnectionManager connectionManager = ConnectionManager.getInstance();
                    Channel channel = connectionManager.getConnection(dubboApplication);
                    if (channel.isActive()) {
                        // 获取ByteBufAllocator用于创建新的ByteBuf
                        CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
                        compositeByteBuf.addComponent(true, dubboPacket.getDubboRequestHeader().getHeaderBytes());
                        compositeByteBuf.addComponent(true, dubboPacket.getBody().bytes());
                        channel.writeAndFlush(compositeByteBuf).addListener((ChannelFutureListener) future -> {
                            if (future.isSuccess()) {
                                ctx.channel().read();
                            } else {
                                // 重试
                                connectionManager.retryQueue(future.channel());
                            }
                        });
                    } else {
                        // 重试
                        connectionManager.retryQueue(channel);
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

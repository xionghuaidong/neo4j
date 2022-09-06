/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.protocol.common.connector.netty;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.bookmark.BookmarkParser;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.protocol.common.handler.BoltChannelInitializer;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.helpers.PortBindException;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPool;
import org.neo4j.server.config.AuthConfigProvider;

/**
 * Provides a connector which provides its services through a domain socket.
 * <p />
 * This implementation is currently limited for internal use only and is enabled via an unsupported switch.
 * <p />
 * Note that protocol level authentication will be unavailable for domain socket based communication. All authorization
 * is provided through the socket file itself and will thus occur on OS level.
 */
public class DomainSocketNettyConnector extends AbstractNettyConnector {
    private final File file;
    private final Config config;
    private final ByteBufAllocator allocator;
    private final ConnectorTransport transport;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final InternalLogProvider logging;

    public DomainSocketNettyConnector(
            String id,
            File file,
            Config config,
            MemoryPool memoryPool,
            ByteBufAllocator allocator,
            EventLoopGroup bossGroup,
            EventLoopGroup workerGroup,
            ConnectorTransport transport,
            Connection.Factory connectionFactory,
            NetworkConnectionTracker connectionTracker,
            BoltProtocolRegistry protocolRegistry,
            Authentication authentication,
            AuthConfigProvider authConfigProvider,
            DefaultDatabaseResolver defaultDatabaseResolver,
            ConnectionHintProvider connectionHintProvider,
            BookmarkParser bookmarkParser,
            InternalLogProvider userLogProvider,
            InternalLogProvider logging) {
        super(
                id,
                new DomainSocketAddress(file),
                memoryPool,
                connectionFactory,
                connectionTracker,
                false,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintProvider,
                bookmarkParser,
                userLogProvider,
                logging);
        if (transport.getDomainSocketChannelType() == null) {
            throw new IllegalArgumentException(
                    "Unsupported transport: " + transport.getName() + " does not support domain sockets");
        }

        this.file = file;
        this.config = config;
        this.allocator = allocator;
        this.transport = transport;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.logging = logging;
    }

    public DomainSocketNettyConnector(
            String id,
            File file,
            Config config,
            MemoryPool memoryPool,
            ByteBufAllocator allocator,
            EventLoopGroup eventLoopGroup,
            ConnectorTransport transport,
            Connection.Factory connectionFactory,
            NetworkConnectionTracker connectionTracker,
            BoltProtocolRegistry protocolRegistry,
            Authentication authentication,
            AuthConfigProvider authConfigProvider,
            DefaultDatabaseResolver defaultDatabaseResolver,
            ConnectionHintProvider connectionHintProvider,
            BookmarkParser bookmarkParser,
            InternalLogProvider userLogProvider,
            InternalLogProvider logging) {
        this(
                id,
                file,
                config,
                memoryPool,
                allocator,
                eventLoopGroup,
                eventLoopGroup,
                transport,
                connectionFactory,
                connectionTracker,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintProvider,
                bookmarkParser,
                userLogProvider,
                logging);
    }

    @Override
    protected EventLoopGroup bossGroup() {
        return this.bossGroup;
    }

    @Override
    protected EventLoopGroup workerGroup() {
        return this.workerGroup;
    }

    @Override
    protected Class<? extends ServerChannel> channelType() {
        return this.transport.getDomainSocketChannelType();
    }

    @Override
    protected ChannelInitializer<Channel> channelInitializer() {
        return new BoltChannelInitializer(this.config, this, this.allocator, this.logging);
    }

    @Override
    protected void onStart() throws Exception {
        super.onStart();

        if (this.file.exists()) {
            if (!this.config.get(BoltConnectorInternalSettings.unsupported_loopback_delete)) {
                throw new PortBindException(
                        this.bindAddress,
                        new BindException("Loopback listen file: " + this.file.getPath() + " already exists."));
            }

            try {
                Files.deleteIfExists(Path.of(file.getPath()));
            } catch (IOException ex) {
                throw new PortBindException(this.bindAddress, ex);
            }
        }
    }

    @Override
    protected void logStartupMessage() {
        userLog.info("Bolt (loopback) enabled on file %s", file);
    }
}

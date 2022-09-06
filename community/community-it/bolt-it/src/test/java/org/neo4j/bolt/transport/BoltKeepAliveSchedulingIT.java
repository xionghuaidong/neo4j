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
package org.neo4j.bolt.transport;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltDefaultWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class BoltKeepAliveSchedulingIT {
    @Inject
    private Neo4jWithSocket server;

    private HostnamePort address;
    private TransportConnection connection;
    private final BoltWire wire = new BoltDefaultWire();

    protected static Consumer<Map<Setting<?>, Object>> getSettingsFunction() {
        return settings -> {
            settings.put(GraphDatabaseSettings.auth_enabled, false);
            settings.put(BoltConnector.connection_keep_alive, Duration.ofMillis(20));
            settings.put(BoltConnector.connection_keep_alive_streaming_scheduling_interval, Duration.ofMillis(10));
        };
    }

    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);
        installSleepProcedure(server.graphDatabaseService());

        address = server.lookupDefaultConnector();
        connection = new SocketConnection(address);
    }

    @Test
    public void shouldSendNoOpForLongRunningTx() throws Exception {
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(wire.hello())
                .send(wire.run("CALL boltissue.sleep()"))
                .send(wire.pull());

        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesSuccess(2)
                .hasReceivedNoopChunks()
                .receivesSuccess();
    }

    private static void installSleepProcedure(GraphDatabaseService db) throws ProcedureException {
        GraphDatabaseAPI dbApi = (GraphDatabaseAPI) db;

        dbApi.getDependencyResolver()
                .resolveDependency(GlobalProcedures.class)
                .register(
                        new CallableProcedure.BasicProcedure(procedureSignature("boltissue", "sleep")
                                .out(ProcedureSignature.VOID)
                                .build()) {
                            @Override
                            public RawIterator<AnyValue[], ProcedureException> apply(
                                    Context context, AnyValue[] objects, ResourceTracker resourceTracker)
                                    throws ProcedureException {
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    throw new ProcedureException(Status.General.UnknownError, e, "Interrupted");
                                }
                                return RawIterator.empty();
                            }
                        });
    }
}

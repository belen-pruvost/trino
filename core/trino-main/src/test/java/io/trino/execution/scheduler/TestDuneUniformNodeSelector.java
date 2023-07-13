/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.trino.Session;
import io.trino.client.NodeVersion;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTask;
import io.trino.metadata.InMemoryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingSplit;
import io.trino.util.FinalizerService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SystemSessionProperties.DUNE_MAX_WORKER_NODES;
import static io.trino.SystemSessionProperties.DUNE_WORKER_NODES_SHUFFLE_SEED;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDuneUniformNodeSelector
{
    private FinalizerService finalizerService;
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;
    private NodeSchedulerConfig nodeSchedulerConfig;
    private NodeScheduler nodeScheduler;
    private NodeSelector nodeSelector;
    private Map<InternalNode, RemoteTask> taskMap;
    private ExecutorService remoteTaskExecutor;
    private ScheduledExecutorService remoteTaskScheduledExecutor;
    private Session session;

    @BeforeEach
    public void setUp()
    {
        session = TestingSession.testSessionBuilder()
                .setSystemProperty(DUNE_MAX_WORKER_NODES, "1")
                .setSystemProperty(DUNE_WORKER_NODES_SHUFFLE_SEED, "2")
                .build();
        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();

        nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setMinPendingSplitsPerTask(10)
                .setMaxAdjustedPendingSplitsWeightPerTask(100)
                .setIncludeCoordinator(false);

        // contents of taskMap indicate the node-task map for the current stage
        nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, nodeTaskMap));
        taskMap = new HashMap<>();
        nodeSelector = nodeScheduler.createNodeSelector(session, Optional.of(TEST_CATALOG_HANDLE));
        remoteTaskExecutor = newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s"));
        remoteTaskScheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("remoteTaskScheduledExecutor-%s"));

        finalizerService.start();
    }

    @AfterEach
    public void tearDown()
    {
        remoteTaskExecutor.shutdown();
        remoteTaskExecutor = null;
        remoteTaskScheduledExecutor.shutdown();
        remoteTaskScheduledExecutor = null;
        nodeSchedulerConfig = null;
        nodeScheduler = null;
        nodeSelector = null;
        finalizerService.destroy();
        finalizerService = null;
    }

    @Test
    public void testWorkerLimiting()
    {
        InternalNode node1 = new InternalNode("node1", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false);
        nodeManager.addNodes(node1);
        InternalNode node2 = new InternalNode("node2", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false);
        nodeManager.addNodes(node2);

        List<InternalNode> singleNode = List.of(nodeSelector.allNodes().getFirst());
        assertThat(nodeSelector.allNodes()).isEqualTo(singleNode);
        assertThat(nodeSelector.selectCurrentNode().isCoordinator()).isTrue();
        assertThat(nodeSelector.selectRandomNodes(2)).isEqualTo(singleNode);

        Set<Split> splits = new LinkedHashSet<>();
        for (int i = 0; i < 20 * 9; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, TestingSplit.createRemoteSplit()));
        }

        Multimap<InternalNode, Split> assignment = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignment.keySet().stream().toList()).isEqualTo(singleNode);
        assertThat(assignment.size()).isEqualTo(20);
    }
}

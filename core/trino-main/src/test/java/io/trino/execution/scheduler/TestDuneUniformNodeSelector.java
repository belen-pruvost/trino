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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.SystemSessionProperties.DUNE_SCHEDULING_MAX_WORKER_NODES_PER_DATA_SHARD;
import static io.trino.SystemSessionProperties.DUNE_SCHEDULING_WORKER_NODES_MODULUS;
import static io.trino.SystemSessionProperties.DUNE_SCHEDULING_WORKER_NODES_SHUFFLE_SEED;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static java.lang.Integer.min;
import static java.lang.String.format;
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
    private Map<InternalNode, RemoteTask> taskMap;
    private ExecutorService remoteTaskExecutor;
    private ScheduledExecutorService remoteTaskScheduledExecutor;

    @BeforeEach
    public void setUp()
    {
        finalizerService = new FinalizerService();
        nodeTaskMap = new NodeTaskMap(finalizerService);
        nodeManager = new InMemoryNodeManager();

        nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setMinPendingSplitsPerTask(10)
                .setMinCandidates(100) // Must be greater than largest number of nodes used to keep tests non-flaky
                .setMaxAdjustedPendingSplitsWeightPerTask(100)
                .setIncludeCoordinator(false);

        // contents of taskMap indicate the node-task map for the current stage
        nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, nodeTaskMap));
        taskMap = new HashMap<>();
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
        finalizerService.destroy();
        finalizerService = null;
    }

    @Test
    public void testWorkerLimiting()
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty(DUNE_SCHEDULING_MAX_WORKER_NODES_PER_DATA_SHARD, "1")
                .setSystemProperty(DUNE_SCHEDULING_WORKER_NODES_SHUFFLE_SEED, "2")
                .setSystemProperty(DUNE_SCHEDULING_WORKER_NODES_MODULUS, "1")
                .build();

        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, Optional.of(TEST_CATALOG_HANDLE));
        InternalNode node1 = new InternalNode("test-workers-1-123", URI.create("http://10.0.0.1:13"), NodeVersion.UNKNOWN, false);
        nodeManager.addNodes(node1);
        InternalNode node2 = new InternalNode("test-workers-2-123", URI.create("http://10.0.0.1:12"), NodeVersion.UNKNOWN, false);
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

    @Test
    public void testSplitAssignments()
    {
        for (int nodeCount = 1; nodeCount < 10; nodeCount++) {
            for (int modulus = 1; modulus < 10; modulus++) {
                for (int nodesPerShard = 1; nodesPerShard < 10; nodesPerShard++) {
                    testSplitAssignment(nodeCount, modulus, nodesPerShard);
                }
            }
        }
        testSplitAssignment(6, 3, 1);
        testSplitAssignment(16, 16, 1);
        testSplitAssignment(32, 16, 1);
        testSplitAssignment(32, 16, 2);
        testSplitAssignment(32, 16, 3);
        testSplitAssignment(31, 16, 1);
        testSplitAssignment(31, 16, 1);
        testSplitAssignment(30, 15, 1);
        testSplitAssignment(30, 15, 2);
        testSplitAssignment(30, 15, 3);
    }

    private void testSplitAssignment(int nodeCount, int modulus, int nodesPerShard)
    {
        Session session = TestingSession.testSessionBuilder()
                .setSystemProperty(DUNE_SCHEDULING_MAX_WORKER_NODES_PER_DATA_SHARD, String.valueOf(nodesPerShard))
                .setSystemProperty(DUNE_SCHEDULING_WORKER_NODES_SHUFFLE_SEED, "2")
                .setSystemProperty(DUNE_SCHEDULING_WORKER_NODES_MODULUS, String.valueOf(modulus))
                .build();

        for (int i = 0; i < nodeCount; i++) {
            nodeManager.addNodes(new InternalNode(format("test-workers-%d-123", i), URI.create("http://10.0.0.1:" + i), NodeVersion.UNKNOWN, false));
        }
        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(session, Optional.of(TEST_CATALOG_HANDLE));

        int minNodesPerGroup = nodeCount / modulus;
        int maxNodesPerGroup = nodeCount / modulus + 1;
        int minGroups = modulus - (nodeCount % modulus);
        int maxGroups = (nodeCount % modulus);
        int selectedNodesCount = min(nodesPerShard, minNodesPerGroup) * minGroups + min(nodesPerShard, maxNodesPerGroup) * maxGroups;

        Set<InternalNode> nodes = new HashSet<>(nodeSelector.allNodes());
        assertThat(nodes.size()).isEqualTo(selectedNodesCount);
        assertThat(nodeSelector.selectCurrentNode().isCoordinator()).isTrue();
        assertThat(new HashSet<>(nodeSelector.selectRandomNodes(selectedNodesCount))).isEqualTo(nodes);

        Set<Split> splits = new LinkedHashSet<>();
        for (int i = 0; i < 20 * selectedNodesCount; i++) {
            splits.add(new Split(TEST_CATALOG_HANDLE, TestingSplit.createRemoteSplit()));
        }

        Multimap<InternalNode, Split> assignment = nodeSelector.computeAssignments(splits, ImmutableList.copyOf(taskMap.values())).getAssignments();
        assertThat(assignment.keySet()).isEqualTo(nodes);
        assertThat(assignment.size()).isEqualTo(selectedNodesCount * 20);

        for (InternalNode node : nodeManager.getAllNodes().getActiveNodes()) {
            if (!node.isCoordinator()) {
                nodeManager.removeNode(node);
            }
        }
    }
}

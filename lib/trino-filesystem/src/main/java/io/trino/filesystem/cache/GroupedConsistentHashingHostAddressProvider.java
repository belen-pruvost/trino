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
package io.trino.filesystem.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.ishugaliy.allgood.consistent.hash.ConsistentHash;
import org.ishugaliy.allgood.consistent.hash.HashRing;
import org.ishugaliy.allgood.consistent.hash.hasher.DefaultHasher;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class GroupedConsistentHashingHostAddressProvider
        implements CachingHostAddressProvider
{
    private static final Logger log = Logger.get(GroupedConsistentHashingHostAddressProvider.class);

    private final NodeManager nodeManager;
    private final int modulus;
    private final ScheduledExecutorService hashRingUpdater = newSingleThreadScheduledExecutor(daemonThreadsNamed("hash-ring-refresher-%s"));
    private final int replicationFactor;
    private final Comparator<HostAddress> hostAddressComparator = Comparator.comparing(HostAddress::getHostText).thenComparing(HostAddress::getPort);

    private final ConsistentHash<TrinoNode> consistentHashRing = HashRing.<TrinoNode>newBuilder()
            .hasher(DefaultHasher.METRO_HASH)
            .build();

    @Inject
    public GroupedConsistentHashingHostAddressProvider(NodeManager nodeManager, ConsistentHashingHostAddressProviderConfig configuration, @Modulus int modulus)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.replicationFactor = configuration.getPreferredHostsCount();
        this.modulus = modulus;
    }

    @Override
    public List<HostAddress> getHosts(String splitPath, List<HostAddress> defaultAddresses)
    {
        return consistentHashRing.locate(splitPath, replicationFactor)
                .stream()
                .flatMap(n -> n.hostAndPorts.stream())
                .sorted(hostAddressComparator)
                .collect(toImmutableList());
    }

    @PostConstruct
    public void startRefreshingHashRing()
    {
        hashRingUpdater.scheduleWithFixedDelay(this::refreshHashRing, 5, 5, TimeUnit.SECONDS);
        refreshHashRing();
    }

    @PreDestroy
    public void destroy()
    {
        hashRingUpdater.shutdownNow();
    }

    @VisibleForTesting
    protected synchronized void refreshHashRing()
    {
        try {
            Set<TrinoNode> nodes = group(modulus, nodeManager.getWorkerNodes()).entrySet().stream().map(TrinoNode::of).collect(toImmutableSet());
            Set<TrinoNode> hashRingNodes = consistentHashRing.getNodes();
            Set<TrinoNode> removedNodes = Sets.difference(hashRingNodes, nodes);
            Set<TrinoNode> newNodes = Sets.difference(nodes, hashRingNodes);
            // Avoid acquiring a write lock in consistentHashRing if possible
            if (!newNodes.isEmpty()) {
                consistentHashRing.addAll(newNodes);
            }
            if (!removedNodes.isEmpty()) {
                removedNodes.forEach(consistentHashRing::remove);
            }
        }
        catch (Exception e) {
            log.error(e, "Error refreshing hash ring");
        }
    }

    private record TrinoNode(String identifier, Set<HostAddress> hostAndPorts)
            implements org.ishugaliy.allgood.consistent.hash.node.Node
    {
        @Override
        public String getKey()
        {
            return identifier;
        }

        public static TrinoNode of(Map.Entry<Integer, Set<Node>> entry)
        {
            return new TrinoNode(String.valueOf(entry.getKey()), entry.getValue().stream().map(Node::getHostAndPort).collect(toImmutableSet()));
        }
    }

    private static Map<Integer, Set<Node>> group(int modulus, Set<Node> workerNodes)
    {
        return workerNodes.stream()
                .collect(Collectors.toUnmodifiableMap(n ->
                                parseNodeNumber(n.getNodeIdentifier())
                                        .map(number -> number % modulus)
                                        .orElse(modulus),
                        Set::of, Sets::union));
    }

    private static final Pattern nodeNumberPattern = Pattern.compile(".*-workers-(\\d+)-\\d+");

    /**
     * Relies on <a href="https://github.com/duneanalytics/arrakis-jobs/blob/85c4272b06a1beb25fa7fc2cfa654c1a9893568e/query/trino/image/src/main/jib/app/scripts/entrypoint.sh#L18">https://github.com/duneanalytics/arrakis-jobs/blob/85c4272b06a1beb25fa7fc2cfa654c1a9893568e/query/trino/image/src/main/jib/app/scripts/entrypoint.sh#L18</a>
     * and the naming convention of kubernetes nodes.
     * <p>
     * Example: nodeIdentifier="paid-10u-y3mzc-1-workers-4-1699782434" -> 4
     */
    private static Optional<Integer> parseNodeNumber(String nodeIdentifier)
    {
        Matcher match = nodeNumberPattern.matcher(nodeIdentifier);
        if (match.matches()) {
            try {
                return Optional.of(Integer.parseInt(match.group(1)));
            }
            catch (NumberFormatException e) {
                log.warn(e, "Failed to parse node identifier '%s'", nodeIdentifier);
                return Optional.empty();
            }
        }
        else {
            log.warn("Failed to parse node identifier '%s'", nodeIdentifier);
            return Optional.empty();
        }
    }

    @Retention(RUNTIME)
    @Target({PARAMETER, METHOD})
    @BindingAnnotation
    public @interface Modulus
    {
    }
}

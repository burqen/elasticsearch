/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.GlobalCheckpointSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.FailureStrategy;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryListener;
import org.elasticsearch.indices.recovery.RecoveryMetricsCollector;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.elasticsearch.indices.cluster.IndicesClusterStateService.INDICES_RECOVERY_LOCAL_RETRY_SETTING;
import static org.elasticsearch.indices.cluster.IndicesClusterStateService.SHARD_LOCK_RETRY_INTERVAL_SETTING;
import static org.elasticsearch.indices.recovery.FailureStrategy.FAIL_SEND;
import static org.elasticsearch.indices.recovery.FailureStrategy.RETRY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/// Unit tests for shard-creation-state markers: each allocation's state carries its retry count so either
/// the retry applier or cluster-state application may recreate the shard with the same count.
public class IndicesClusterStateServiceRetryContextTests extends AbstractIndicesClusterStateServiceTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ClusterApplierService clusterApplierService;
    private ShardStateAction shardStateAction;
    private RecordingIndicesService indicesService;
    private IndicesClusterStateService indicesClusterStateService;
    private ClusterState state;
    private final Queue<PendingApplierTask> pendingApplierTasks = new ArrayDeque<>();

    private record PendingApplierTask(String source, Consumer<ClusterState> consumer, ActionListener<Void> listener) {}

    @Before
    public void setUpService() {
        disableRandomFailures();
        threadPool = new TestThreadPool(getTestName());
        pendingApplierTasks.clear();
        shardStateAction = mock(ShardStateAction.class);
        clusterService = mock(ClusterService.class);
        clusterApplierService = mock(ClusterApplierService.class);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        doAnswer(invocation -> {
            pendingApplierTasks.add(
                new PendingApplierTask(invocation.getArgument(0), invocation.getArgument(2), invocation.getArgument(3))
            );
            return null;
        }).when(clusterApplierService).runOnApplierThread(anyString(), any(Priority.class), any(), any());

        indicesClusterStateService = createService(true);
        indicesClusterStateService.start();
    }

    @After
    public void tearDownService() {
        if (indicesClusterStateService != null) {
            indicesClusterStateService.close();
        }
        assertThat(ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS), equalTo(true));
    }

    /// Validate the test setup with FAIL_SEND strategy
    public void testFailSendNotifyMaster() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));

        MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
        assertNotNull(shard);
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("simulated recovery failure"),
            shard.recoveryState(),
            currentInFlight(shardRouting)
        );

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));

        assertTrue(isFailedToMaster(shardRouting));
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
        assertThat(pendingApplierTasks.size(), equalTo(0));
    }

    public void testRetrySetContextWithoutNotifyingMaster() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));

        handleRecoveryFailureWithRetry(shardRouting);

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(awaitingRetryCount(shardRouting), equalTo(1));
        assertFalse(isFailedToMaster(shardRouting));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
        assertThat(pendingApplierTasks.size(), equalTo(1));
        assertThat(pendingApplierTasks.peek().source(), equalTo("retry recovery " + shardRouting.shardId()));
    }

    public void testRetryApplierRecreatesShardAndClearsContext() {
        ShardRouting shardRouting = applyInitializingPrimary();
        int createsBefore = indicesService.createShardCalls.get();

        handleRecoveryFailureWithRetry(shardRouting);
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertTrue(isAwaitingRetry(shardRouting));

        drainApplierTasks();

        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));
        // InFlight now spans recovery too: the retried attempt's recovery is still pending (no
        // onRecoveryDone/onRecoveryFailure has fired yet), so the ticket is still held, not cleared.
        assertThat(
            indicesClusterStateService.shardCreationStates.get(creationKey(shardRouting)),
            instanceOf(IndicesClusterStateService.ShardCreationState.InFlight.class)
        );
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore + 1));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testConsecutiveRetriesIncrementLocalRetries() {
        ShardRouting shardRouting = applyInitializingPrimary();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(0));

        failRecoveryViaListener(RETRY);
        assertThat(awaitingRetryCount(shardRouting), equalTo(1));
        drainApplierTasks();
        shardRouting = indicesService.getShardOrNull(shardRouting.shardId()).routingEntry();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));

        failRecoveryViaListener(RETRY);
        assertThat(awaitingRetryCount(shardRouting), equalTo(2));
        drainApplierTasks();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(2));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testGiveUpResetsLocalRetriesToZero() {
        ShardRouting shardRouting = applyInitializingPrimary();
        failRecoveryViaListener(RETRY);
        drainApplierTasks();
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));

        // New allocation id → context cleared; cluster-state create owns recreate with localRecoveryRetries=0.
        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        assertFalse(newAllocation.isSameAllocation(shardRouting));
        applyState(stateWithShardRouting(newAllocation));

        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(0));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
    }

    public void testRetryDisabledBecomesFailSend() {
        indicesClusterStateService.close();
        indicesClusterStateService = createService(false);
        indicesClusterStateService.start();

        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);

        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertFalse(isAwaitingRetry(shardRouting));
        assertTrue(isFailedToMaster(shardRouting));
        assertTrue(pendingApplierTasks.isEmpty());
        verify(shardStateAction).localShardFailed(eq(shardRouting), anyString(), any(), any(), any());
    }

    public void testClusterStateCreateWhileContextAppliesLocalRetries() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        int createsAfterFail = indicesService.createShardCalls.get();
        assertTrue(isAwaitingRetry(shardRouting));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));

        // Intervening cluster-state apply may recreate; context supplies localRecoveryRetries.
        applyState(ClusterState.builder(state).version(state.version() + 1).build());
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterFail + 1));
        // InFlight now spans recovery too: recovery for this CS-apply-created attempt is still pending.
        assertThat(
            indicesClusterStateService.shardCreationStates.get(creationKey(shardRouting)),
            instanceOf(IndicesClusterStateService.ShardCreationState.InFlight.class)
        );

        // Retry applier still pending; context already cleared so it must not create again.
        assertThat(pendingApplierTasks.size(), equalTo(1));
        int createsAfterCs = indicesService.createShardCalls.get();
        drainApplierTasks();
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterCs));
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testGiveUpOnAllocationChangeThenClusterStateCreates() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRetry(shardRouting));

        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        assertFalse(newAllocation.isSameAllocation(shardRouting));
        applyState(stateWithShardRouting(newAllocation));

        assertFalse(indicesClusterStateService.shardCreationStates.containsKey(creationKey(shardRouting)));
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
        // Retry applier task is still pending; when drained it must give up without another create.
        assertThat(pendingApplierTasks.size(), equalTo(1));
        int createsAfterCs = indicesService.createShardCalls.get();
        drainApplierTasks();
        assertFalse(indicesClusterStateService.shardCreationStates.containsKey(creationKey(shardRouting)));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterCs));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
    }

    public void testUpdateRetryContextClearsWhenShardNotOnLocalNode() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRetry(shardRouting));

        applyState(stateWithShardMovedOffLocalNode(shardRouting));
        assertTrue(indicesClusterStateService.shardCreationStates.isEmpty());
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
    }

    public void testUpdateRetryContextClearsWhenAllocationIdChanges() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRetry(shardRouting));

        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        applyState(stateWithShardRouting(newAllocation));
        assertFalse(indicesClusterStateService.shardCreationStates.containsKey(creationKey(shardRouting)));
    }

    public void testUpdateRetryContextClearsWhenNotInitializing() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRetry(shardRouting));

        ShardRouting started = TestShardRouting.shardRoutingBuilder(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.STARTED
        ).withAllocationId(shardRouting.allocationId()).build();
        applyState(stateWithShardRouting(started));
        assertFalse(isAwaitingRetry(shardRouting));
        // Master thinks this allocation is active but we have no shard for it (it was removed by the
        // earlier recovery failure) -- createIndicesAndUpdateShards reports it as failed separately.
        assertTrue(isFailedToMaster(shardRouting));
    }

    // NOTE: a test formerly lived here ("...ClearsWhenFailedShardsCacheHit") simulating a FailedToMaster
    // write landing on top of an existing AwaitingRetry via a second direct handleRecoveryFailure(FAIL_SEND)
    // call. That scenario is no longer constructible: handleRecoveryFailure now requires the caller's
    // ticket to match the live shardCreationStates entry, and a single InFlight attempt can only ever
    // produce one terminal signal (RecoveryListener#assertOnce), so two independent handleRecoveryFailure
    // calls for the same attempt can't happen. The only remaining production path that can still write
    // FailedToMaster over a live AwaitingRetry is FailedShardHandler (a separate, ticket-independent
    // caller -- see the "leave FailedShardHandler as is" decision). testRetryDoesNotClobberFailedToMaster
    // covers the mirror-image ordering (FailedToMaster already present, then a stale RETRY arrives).

    public void testRetrySkipsWhenIndexServiceMissingKeepsMarker() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRetry(shardRouting));
        int createsBefore = indicesService.createShardCalls.get();

        indicesService.removeIndex(
            shardRouting.index(),
            IndexRemovalReason.NO_LONGER_ASSIGNED,
            "test",
            Runnable::run,
            ActionListener.noop()
        );
        assertNull(indicesService.indexService(shardRouting.index()));

        drainApplierTasks();
        assertTrue(isAwaitingRetry(shardRouting));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));

        applyState(ClusterState.builder(state).version(state.version() + 1).build());
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));
        assertFalse(isAwaitingRetry(shardRouting));
    }

    public void testRetryContextKeptWhenCreateGivesUpMissingPeerSource() {
        String indexName = randomIndexName();
        ClusterState base = ClusterStateCreationUtils.state(indexName, false, ShardRoutingState.STARTED, ShardRoutingState.INITIALIZING);
        String localNodeId = base.nodes().getLocalNodeId();
        Index index = base.metadata().getProject().index(indexName).getIndex();
        ShardRouting primary = base.routingTable().index(index).shard(0).primaryShard();
        assertTrue(primary.active());
        assertFalse(primary.currentNodeId().equals(localNodeId));
        ShardRouting replica = TestShardRouting.newShardRouting(primary.shardId(), localNodeId, false, ShardRoutingState.INITIALIZING);
        assertThat(replica.recoverySource().getType(), equalTo(RecoverySource.Type.PEER));
        applyState(
            ClusterState.builder(base)
                .routingTable(
                    RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(primary).addShard(replica).build()).build()
                )
                .build()
        );
        assertNotNull(indicesService.getShardOrNull(replica.shardId()));

        handleRecoveryFailureWithRetry(replica);
        assertTrue(isAwaitingRetry(replica));

        // Remove node holding primary and make sure master is an existing node
        applyState(
            ClusterState.builder(state)
                .nodes(
                    DiscoveryNodes.builder(state.nodes())
                        .remove(primary.currentNodeId())
                        .masterNodeId(state.nodes().getLocalNodeId())
                        .build()
                )
                .build()
        );
        // CS apply may attempt create; missing peer source → early return, keep marker for localRecoveryRetries.
        assertTrue(isAwaitingRetry(replica));

        int createsBefore = indicesService.createShardCalls.get();
        drainApplierTasks();
        assertTrue(isAwaitingRetry(replica));
        assertThat(awaitingRetryCount(replica), equalTo(1));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsBefore));
        assertNull(indicesService.getShardOrNull(replica.shardId()));
    }

    public void testRetryContextClearedWhenCreateFails() {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertTrue(isAwaitingRetry(shardRouting));

        indicesService.failNextCreateShard = new IOException("simulated create failure");
        drainApplierTasks();

        assertFalse(isAwaitingRetry(shardRouting));
        assertNull(indicesService.getShardOrNull(shardRouting.shardId()));
        // create failure path notifies master via failAndRemoveShard(sendShardFailure=true)
        verify(shardStateAction).localShardFailed(any(), anyString(), any(), any(), any());
        assertTrue(isFailedToMaster(shardRouting));
    }

    public void testRetryUsesCurrentRoutingFromClusterState() {
        ShardRouting shardRouting = applyInitializingPrimary();
        ShardId shardId = shardRouting.shardId();
        String otherNodeId = null;
        for (DiscoveryNode node : state.nodes()) {
            if (node.getId().equals(state.nodes().getLocalNodeId()) == false) {
                otherNodeId = node.getId();
                break;
            }
        }
        assertNotNull(otherNodeId);

        final var withRelocating = TestShardRouting.shardRoutingBuilder(
            shardId,
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withAllocationId(AllocationId.newRelocation(shardRouting.allocationId())).withRelocatingNodeId(otherNodeId).build();
        applyState(stateWithShardRouting(withRelocating));
        assertNotNull(indicesService.getShardOrNull(shardId));

        handleRecoveryFailureWithRetry(withRelocating);
        assertThat(awaitingRetryCount(shardRouting), equalTo(1));

        final var clearedRelocating = TestShardRouting.shardRoutingBuilder(
            shardId,
            withRelocating.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        ).withAllocationId(AllocationId.finishRelocation(withRelocating.allocationId())).build();
        assertTrue(clearedRelocating.isSameAllocation(withRelocating));
        assertNotEquals(clearedRelocating.allocationId(), withRelocating.allocationId());
        assertThat(clearedRelocating.relocatingNodeId(), nullValue());
        // Publish routing change without CS create so the retry applier is what recreates,
        // proving the retry path passes currentRouting (relocating cleared) into createShard.
        state = stateWithShardRouting(clearedRelocating);
        when(clusterService.state()).thenReturn(state);
        assertTrue(isAwaitingRetry(shardRouting));

        drainApplierTasks();

        MockIndexShard recreated = indicesService.getShardOrNull(shardId);
        assertNotNull(recreated);
        assertThat(recreated.routingEntry().relocatingNodeId(), nullValue());
        assertThat(recreated.routingEntry().allocationId(), equalTo(clearedRelocating.allocationId()));
        assertThat(recreated.recoveryState().getLocalRetries(), equalTo(1));
        assertFalse(isAwaitingRetry(shardRouting));
    }

    public void testRetryDoesNotRegressAnExistingFresherMarker() {
        final var shardRouting = applyInitializingPrimary();
        // Capture the original ticket before it's spent, so the later "stale" call below can present it.
        final var originalInFlight = currentInFlight(shardRouting);
        handleRecoveryFailureWithRetry(shardRouting);
        assertThat(awaitingRetryCount(shardRouting), equalTo(1));

        // Seed a fresher marker, as if a newer attempt had already recorded more retries.
        indicesClusterStateService.shardCreationStates.put(
            creationKey(shardRouting),
            new IndicesClusterStateService.ShardCreationState.AwaitingRetry(5)
        );
        int tasksBefore = pendingApplierTasks.size();

        // A late failure from the original (now-superseded) attempt, presenting its own now-stale
        // ticket, must not regress the marker nor schedule a second retry task.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            RETRY,
            primaryTerm(shardRouting),
            new Exception("stale failure"),
            recoveryState(shardRouting, 0),
            originalInFlight
        );
        assertThat(awaitingRetryCount(shardRouting), equalTo(5));
        assertThat(pendingApplierTasks.size(), equalTo(tasksBefore));
    }

    public void testRetryDoesNotClobberFailedToMaster() {
        final var shardRouting = applyInitializingPrimary();
        final var inFlight = currentInFlight(shardRouting);

        // A failure already reported to master for this exact allocation.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            FAIL_SEND,
            primaryTerm(shardRouting),
            new Exception("already failed to master"),
            recoveryState(shardRouting, 0),
            inFlight
        );
        assertTrue(isFailedToMaster(shardRouting));
        int tasksBefore = pendingApplierTasks.size();

        // A second call presenting the same (now-spent) ticket must be a no-op: FailedToMaster survives.
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            RETRY,
            primaryTerm(shardRouting),
            new Exception("stale failure"),
            recoveryState(shardRouting, 0),
            inFlight
        );
        assertTrue(isFailedToMaster(shardRouting));
        assertFalse(isAwaitingRetry(shardRouting));
        assertThat(pendingApplierTasks.size(), equalTo(tasksBefore));
    }

    public void testLateFailureFromPreviousAllocationPreservesReplacementRetry() {
        final var oldRouting = applyInitializingPrimary();
        final var oldListener = indicesService.lastRecoveryListener;
        final var oldRecoveryState = indicesService.getShardOrNull(oldRouting.shardId()).recoveryState();
        final var replacement = TestShardRouting.newShardRouting(
            oldRouting.shardId(),
            oldRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        applyState(stateWithShardRouting(replacement));
        failRecoveryViaListener(RETRY);

        oldListener.onRecoveryFailure(
            oldRecoveryState,
            new RecoveryFailedException(oldRecoveryState, "late failure", new IOException("old attempt")),
            RETRY
        );
        assertThat(awaitingRetryCount(replacement), equalTo(1));

        drainApplierTasks();

        final var recreated = indicesService.getShardOrNull(replacement.shardId());
        assertNotNull(recreated);
        assertTrue(recreated.routingEntry().isSameAllocation(replacement));
        assertThat(recreated.recoveryState().getLocalRetries(), equalTo(1));
        assertFalse(isAwaitingRetry(replacement));
        verify(shardStateAction, never()).localShardFailed(any(), anyString(), any(), any(), any());
    }

    public void testLockWaitSurvivesUnrelatedClusterStateUpdate() throws Exception {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertThat(awaitingRetryCount(shardRouting), equalTo(1));

        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");
        final var recoveryRetry = pendingApplierTasks.remove();
        recoveryRetry.consumer().accept(state);
        recoveryRetry.listener().onResponse(null);
        assertBusy(() -> assertFalse(pendingApplierTasks.isEmpty()));

        // An unrelated cluster-state update (same allocation, just a different stateUUID) while the
        // lock-wait is pending must not cause the continuation to give up -- only a genuine
        // allocation change should. The claim survives and uses the fresh state once it wakes up.
        state = ClusterState.builder(state).stateUUID(UUIDs.randomBase64UUID()).build();
        when(clusterService.state()).thenReturn(state);

        final var lockRetry = pendingApplierTasks.remove();
        lockRetry.consumer().accept(state);
        lockRetry.listener().onResponse(null);

        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        assertThat(indicesService.getShardOrNull(shardRouting.shardId()).recoveryState().getLocalRetries(), equalTo(1));
        assertFalse(isAwaitingRetry(shardRouting));
    }

    public void testLockWaitGivesUpWhenAllocationGoesStale() throws Exception {
        ShardRouting shardRouting = applyInitializingPrimary();
        handleRecoveryFailureWithRetry(shardRouting);
        assertThat(awaitingRetryCount(shardRouting), equalTo(1));

        indicesService.failNextCreateShard = new ShardLockObtainFailedException(shardRouting.shardId(), "test lock held");
        final var recoveryRetry = pendingApplierTasks.remove();
        recoveryRetry.consumer().accept(state);
        recoveryRetry.listener().onResponse(null);
        assertBusy(() -> assertFalse(pendingApplierTasks.isEmpty()));

        // A genuine allocation change while the lock-wait is pending must cause the continuation to
        // give up quietly and clean up its own claim, rather than attempt to create a shard for an
        // allocation that no longer exists.
        ShardRouting newAllocation = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            shardRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING
        );
        applyState(stateWithShardRouting(newAllocation));
        int createsAfterCs = indicesService.createShardCalls.get();

        final var lockRetry = pendingApplierTasks.remove();
        lockRetry.consumer().accept(state);
        lockRetry.listener().onResponse(null);

        assertFalse(isAwaitingRetry(shardRouting));
        assertThat(indicesService.createShardCalls.get(), equalTo(createsAfterCs));
        assertThat(
            indicesService.getShardOrNull(shardRouting.shardId()).routingEntry().allocationId(),
            equalTo(newAllocation.allocationId())
        );
    }

    // --- helpers ---

    private IndicesClusterStateService createService(boolean localRetryEnabled) {
        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(INDICES_RECOVERY_LOCAL_RETRY_SETTING);
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_LOCAL_RETRY_SETTING.getKey(), localRetryEnabled)
            .put(SHARD_LOCK_RETRY_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(1))
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        state = ClusterState.EMPTY_STATE;
        when(clusterService.state()).thenAnswer(invocation -> state);

        indicesService = new RecordingIndicesService();
        return new IndicesClusterStateService(
            settings,
            indicesService,
            clusterService,
            threadPool,
            null,
            shardStateAction,
            null,
            null,
            null,
            null,
            mock(PrimaryReplicaSyncer.class),
            RetentionLeaseSyncer.EMPTY,
            null,
            RecoveryMetricsCollector.NOOP
        );
    }

    private ShardRouting applyInitializingPrimary() {
        applyState(ClusterStateCreationUtils.state(randomIndexName(), true, ShardRoutingState.INITIALIZING));
        ShardRouting shardRouting = state.getRoutingNodes().node(state.nodes().getLocalNodeId()).iterator().next();
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.initializing());
        assertNotNull(indicesService.getShardOrNull(shardRouting.shardId()));
        return shardRouting;
    }

    private void handleRecoveryFailureWithRetry(ShardRouting shardRouting) {
        MockIndexShard shard = indicesService.getShardOrNull(shardRouting.shardId());
        assertNotNull(shard);
        indicesClusterStateService.handleRecoveryFailure(
            shardRouting,
            RETRY,
            primaryTerm(shardRouting),
            new Exception("simulated recovery failure"),
            shard.recoveryState(),
            currentInFlight(shardRouting)
        );
    }

    private static IndicesClusterStateService.ShardCreationKey creationKey(ShardRouting routing) {
        return new IndicesClusterStateService.ShardCreationKey(routing.shardId(), routing.allocationId().getId());
    }

    /// Fetches the currently-claimed [IndicesClusterStateService.ShardCreationState.InFlight] ticket for
    /// {@code routing}'s allocation, for tests that call [IndicesClusterStateService#handleRecoveryFailure]
    /// directly rather than through the real [RecoveryListener].
    private IndicesClusterStateService.ShardCreationState.InFlight currentInFlight(ShardRouting routing) {
        var current = indicesClusterStateService.shardCreationStates.get(creationKey(routing));
        assertThat(current, instanceOf(IndicesClusterStateService.ShardCreationState.InFlight.class));
        return (IndicesClusterStateService.ShardCreationState.InFlight) current;
    }

    private boolean isAwaitingRetry(ShardRouting routing) {
        return indicesClusterStateService.shardCreationStates.get(
            creationKey(routing)
        ) instanceof IndicesClusterStateService.ShardCreationState.AwaitingRetry;
    }

    private Integer awaitingRetryCount(ShardRouting routing) {
        var current = indicesClusterStateService.shardCreationStates.get(creationKey(routing));
        return current instanceof IndicesClusterStateService.ShardCreationState.AwaitingRetry(var n) ? n : null;
    }

    private boolean isFailedToMaster(ShardRouting routing) {
        return indicesClusterStateService.shardCreationStates.get(
            creationKey(routing)
        ) instanceof IndicesClusterStateService.ShardCreationState.FailedToMaster;
    }

    private RecoveryState recoveryState(ShardRouting shardRouting, int localRetries) {
        return new RecoveryState(shardRouting, state.nodes().getLocalNode(), null, localRetries);
    }

    /// Drives the production increment path via [RecoveryListener#onRecoveryFailure]
    /// rather than calling [IndicesClusterStateService#handleRecoveryFailure] directly.
    private void failRecoveryViaListener(FailureStrategy failureStrategy) {
        RecoveryListener listener = indicesService.lastRecoveryListener;
        assertNotNull("createShard must have captured a RecoveryListener", listener);
        MockIndexShard shard = indicesService.getShardOrNull(indicesService.lastCreatedShardId);
        assertNotNull(shard);
        RecoveryState recoveryState = shard.recoveryState();
        listener.onRecoveryFailure(
            recoveryState,
            new RecoveryFailedException(recoveryState, "simulated", new Exception("simulated")),
            failureStrategy
        );
    }

    private long primaryTerm(ShardRouting shardRouting) {
        return state.metadata().getProject().index(shardRouting.index()).primaryTerm(shardRouting.id());
    }

    private void applyState(ClusterState newState) {
        ClusterState previous = state;
        state = newState;
        when(clusterService.state()).thenReturn(state);
        indicesClusterStateService.applyClusterState(new ClusterChangedEvent("test", state, previous));
    }

    private void drainApplierTasks() {
        while (pendingApplierTasks.isEmpty() == false) {
            PendingApplierTask task = pendingApplierTasks.poll();
            task.consumer().accept(state);
            task.listener().onResponse(null);
        }
    }

    private ClusterState stateWithShardRouting(ShardRouting shardRouting) {
        Index index = shardRouting.index();
        IndexMetadata indexMetadata = state.metadata().getProject().index(index);
        assertNotNull("index metadata must exist for " + index, indexMetadata);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(shardRouting).build();
        return ClusterState.builder(state).routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
    }

    private ClusterState stateWithShardMovedOffLocalNode(ShardRouting shardRouting) {
        Index index = shardRouting.index();
        IndexMetadata indexMetadata = state.metadata().getProject().index(index);
        String otherNodeId = null;
        for (DiscoveryNode node : state.nodes()) {
            if (node.getId().equals(state.nodes().getLocalNodeId()) == false) {
                otherNodeId = node.getId();
                break;
            }
        }
        assertNotNull(otherNodeId);
        ShardRouting elsewhere = TestShardRouting.newShardRouting(
            shardRouting.shardId(),
            otherNodeId,
            true,
            ShardRoutingState.INITIALIZING
        );
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(elsewhere).build();
        return ClusterState.builder(state)
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .metadata(Metadata.builder(state.metadata()).put(indexMetadata, false))
            .build();
    }

    private class RecordingIndicesService extends MockIndicesService {
        final AtomicInteger createShardCalls = new AtomicInteger();
        volatile Exception failNextCreateShard;
        volatile RecoveryListener lastRecoveryListener;
        volatile ShardId lastCreatedShardId;
        Runnable afterShardCreated;

        @Override
        protected boolean autoCompleteRecovery() {
            // This test drives recovery completion/failure manually via lastRecoveryListener/failRecoveryViaListener.
            return false;
        }

        @Override
        public void createShard(
            ProjectId projectId,
            ShardRouting shardRouting,
            PeerRecoveryTargetService recoveryTargetService,
            RecoveryListener recoveryListener,
            RepositoriesService repositoriesService,
            Consumer<IndexShard.ShardFailure> onShardFailure,
            GlobalCheckpointSyncer globalCheckpointSyncer,
            RetentionLeaseSyncer retentionLeaseSyncer,
            DiscoveryNode targetNode,
            DiscoveryNode sourceNode,
            long clusterStateVersion,
            int localRecoveryRetries
        ) throws IOException {
            createShardCalls.incrementAndGet();
            lastRecoveryListener = recoveryListener;
            lastCreatedShardId = shardRouting.shardId();
            Exception toFail = failNextCreateShard;
            if (toFail != null) {
                failNextCreateShard = null;
                if (toFail instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                if (toFail instanceof IOException ioe) {
                    throw ioe;
                }
                throw new IOException(toFail);
            }
            super.createShard(
                projectId,
                shardRouting,
                recoveryTargetService,
                recoveryListener,
                repositoriesService,
                onShardFailure,
                globalCheckpointSyncer,
                retentionLeaseSyncer,
                targetNode,
                sourceNode,
                clusterStateVersion,
                localRecoveryRetries
            );
            final var callback = afterShardCreated;
            afterShardCreated = null;
            if (callback != null) {
                callback.run();
            }
        }
    }
}

package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.ArchivePostingBatchRequest;
import com.acme.ledger.v1.ArchivePostingBatchResponse;
import com.acme.ledger.v1.InspectPostingBatchRequest;
import com.acme.ledger.v1.InspectPostingBatchResponse;
import com.acme.ledger.v1.LockPostingBatchRequest;
import com.acme.ledger.v1.LockPostingBatchResponse;
import com.acme.ledger.v1.PostingBatchAdminServiceGrpc;
import com.acme.ledger.v1.PurgePostingBatchRequest;
import com.acme.ledger.v1.PurgePostingBatchResponse;
import com.acme.ledger.v1.ReassignPostingBatchRequest;
import com.acme.ledger.v1.ReassignPostingBatchResponse;
import com.acme.ledger.v1.RepairPostingBatchRequest;
import com.acme.ledger.v1.RepairPostingBatchResponse;
import com.acme.ledger.v1.RestorePostingBatchRequest;
import com.acme.ledger.v1.RestorePostingBatchResponse;
import com.acme.ledger.v1.TransferPostingBatchRequest;
import com.acme.ledger.v1.TransferPostingBatchResponse;
import com.acme.ledger.v1.UnlockPostingBatchRequest;
import com.acme.ledger.v1.UnlockPostingBatchResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Operator actions on posting batches that support staff run by hand.
 */
@GrpcService
public class PostingBatchAdminEndpoint extends PostingBatchAdminServiceGrpc.PostingBatchAdminServiceImplBase {
    protected final PostingBatchAdminApplication application;

    public PostingBatchAdminEndpoint(PostingBatchAdminApplication application) {
        this.application = application;
    }

    /**
     * Archives a posting batch.
     */
    @Override
    public void archivePostingBatch(ArchivePostingBatchRequest request, StreamObserver<ArchivePostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.archivePostingBatch(request));
    }

    /**
     * Restores a posting batch.
     */
    @Override
    public void restorePostingBatch(RestorePostingBatchRequest request, StreamObserver<RestorePostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.restorePostingBatch(request));
    }

    /**
     * Deletes a posting batch.
     */
    @Override
    public void purgePostingBatch(PurgePostingBatchRequest request, StreamObserver<PurgePostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.purgePostingBatch(request));
    }

    /**
     * Reassigns a posting batch.
     */
    @Override
    public void reassignPostingBatch(ReassignPostingBatchRequest request, StreamObserver<ReassignPostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.reassignPostingBatch(request));
    }

    /**
     * Locks a posting batch.
     */
    @Override
    public void lockPostingBatch(LockPostingBatchRequest request, StreamObserver<LockPostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.lockPostingBatch(request));
    }

    /**
     * Unlocks a posting batch.
     */
    @Override
    public void unlockPostingBatch(UnlockPostingBatchRequest request, StreamObserver<UnlockPostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.unlockPostingBatch(request));
    }

    /**
     * Transfers a posting batch.
     */
    @Override
    public void transferPostingBatch(TransferPostingBatchRequest request, StreamObserver<TransferPostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.transferPostingBatch(request));
    }

    /**
     * Repairs a posting batch.
     */
    @Override
    public void repairPostingBatch(RepairPostingBatchRequest request, StreamObserver<RepairPostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.repairPostingBatch(request));
    }

    /**
     * Returns a posting batch.
     */
    @Override
    public void inspectPostingBatch(InspectPostingBatchRequest request, StreamObserver<InspectPostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectPostingBatch(request));
    }
}

package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.AnnotatePostingBatchVersionRequest;
import com.acme.ledger.v1.AnnotatePostingBatchVersionResponse;
import com.acme.ledger.v1.ComparePostingBatchVersionsRequest;
import com.acme.ledger.v1.ComparePostingBatchVersionsResponse;
import com.acme.ledger.v1.GetPostingBatchVersionRequest;
import com.acme.ledger.v1.GetPostingBatchVersionResponse;
import com.acme.ledger.v1.ListPostingBatchVersionsRequest;
import com.acme.ledger.v1.ListPostingBatchVersionsResponse;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.ledger.v1.PrunePostingBatchVersionsRequest;
import com.acme.ledger.v1.PrunePostingBatchVersionsResponse;
import com.acme.ledger.v1.RecordPostingBatchVersionRequest;
import com.acme.ledger.v1.RecordPostingBatchVersionResponse;
import com.acme.ledger.v1.RevertPostingBatchRequest;
import com.acme.ledger.v1.RevertPostingBatchResponse;
import io.grpc.stub.StreamObserver;

/**
 * Keeps every version of a posting batch and restores earlier ones.
 */
public abstract class AbstractPostingBatchHistoryEndpoint extends PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceImplBase {
    protected final PostingBatchHistoryApplication application;

    protected AbstractPostingBatchHistoryEndpoint(PostingBatchHistoryApplication application) {
        this.application = application;
    }

    /**
     * Records a posting batch.
     */
    @Override
    public void recordPostingBatchVersion(RecordPostingBatchVersionRequest request, StreamObserver<RecordPostingBatchVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recordPostingBatchVersion(request));
    }

    /**
     * Lists posting batches a page at a time.
     */
    @Override
    public void listPostingBatchVersions(ListPostingBatchVersionsRequest request, StreamObserver<ListPostingBatchVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPostingBatchVersions(request));
    }

    /**
     * Returns a posting batch.
     */
    @Override
    public void getPostingBatchVersion(GetPostingBatchVersionRequest request, StreamObserver<GetPostingBatchVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPostingBatchVersion(request));
    }

    /**
     * Compares a posting batch.
     */
    @Override
    public void comparePostingBatchVersions(ComparePostingBatchVersionsRequest request, StreamObserver<ComparePostingBatchVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.comparePostingBatchVersions(request));
    }

    /**
     * Reverts a posting batch.
     */
    @Override
    public void revertPostingBatch(RevertPostingBatchRequest request, StreamObserver<RevertPostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.revertPostingBatch(request));
    }

    /**
     * Annotates a posting batch.
     */
    @Override
    public void annotatePostingBatchVersion(AnnotatePostingBatchVersionRequest request, StreamObserver<AnnotatePostingBatchVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.annotatePostingBatchVersion(request));
    }

    /**
     * Deletes a posting batch.
     */
    @Override
    public void prunePostingBatchVersions(PrunePostingBatchVersionsRequest request, StreamObserver<PrunePostingBatchVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.prunePostingBatchVersions(request));
    }
}

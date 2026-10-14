package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CheckPostingBatchEligibilityRequest;
import com.acme.ledger.v1.CheckPostingBatchEligibilityResponse;
import com.acme.ledger.v1.GetPostingBatchRulesRequest;
import com.acme.ledger.v1.GetPostingBatchRulesResponse;
import com.acme.ledger.v1.LintPostingBatchesRequest;
import com.acme.ledger.v1.LintPostingBatchesResponse;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.ledger.v1.ValidatePostingBatchRequest;
import com.acme.ledger.v1.ValidatePostingBatchResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks posting batches against the rules before anything acts on them.
 */
@GrpcService
public class PostingBatchValidationEndpoint extends PostingBatchValidationServiceGrpc.PostingBatchValidationServiceImplBase {
    protected final PostingBatchValidationApplication application;

    public PostingBatchValidationEndpoint(PostingBatchValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a posting batch.
     */
    @Override
    public void validatePostingBatch(ValidatePostingBatchRequest request, StreamObserver<ValidatePostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.validatePostingBatch(request));
    }

    /**
     * Previews a posting batch.
     */
    @Override
    public void previewPostingBatch(PreviewPostingBatchRequest request, StreamObserver<PreviewPostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewPostingBatch(request));
    }

    /**
     * Checks a posting batch.
     */
    @Override
    public void checkPostingBatchEligibility(CheckPostingBatchEligibilityRequest request, StreamObserver<CheckPostingBatchEligibilityResponse> observer) {
        RpcSupport.unary(observer, () -> application.checkPostingBatchEligibility(request));
    }

    /**
     * Scores a posting batch.
     */
    @Override
    public void scorePostingBatch(ScorePostingBatchRequest request, StreamObserver<ScorePostingBatchResponse> observer) {
        RpcSupport.unary(observer, () -> application.scorePostingBatch(request));
    }

    /**
     * Lints a posting batch.
     */
    @Override
    public void lintPostingBatches(LintPostingBatchesRequest request, StreamObserver<LintPostingBatchesResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintPostingBatches(request));
    }

    /**
     * Returns a posting batch.
     */
    @Override
    public void getPostingBatchRules(GetPostingBatchRulesRequest request, StreamObserver<GetPostingBatchRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPostingBatchRules(request));
    }
}

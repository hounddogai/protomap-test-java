package com.acme.finance.domains.assets;

import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.GetDisposalRulesRequest;
import com.acme.assets.v1.GetDisposalRulesResponse;
import com.acme.assets.v1.LintDisposalsRequest;
import com.acme.assets.v1.LintDisposalsResponse;
import com.acme.assets.v1.NormalizeDisposalRequest;
import com.acme.assets.v1.NormalizeDisposalResponse;
import com.acme.assets.v1.PreviewDisposalRequest;
import com.acme.assets.v1.PreviewDisposalResponse;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.ValidateDisposalRequest;
import com.acme.assets.v1.ValidateDisposalResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Checks disposals against the rules before anything acts on them.
 */
public abstract class AbstractDisposalValidationEndpoint extends DisposalValidationServiceGrpc.DisposalValidationServiceImplBase {
    protected final DisposalValidationApplication application;

    protected AbstractDisposalValidationEndpoint(DisposalValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates a disposal.
     */
    @Override
    public void validateDisposal(ValidateDisposalRequest request, StreamObserver<ValidateDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateDisposal(request));
    }

    /**
     * Previews a disposal.
     */
    @Override
    public void previewDisposal(PreviewDisposalRequest request, StreamObserver<PreviewDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewDisposal(request));
    }

    /**
     * Normalizes a disposal.
     */
    @Override
    public void normalizeDisposal(NormalizeDisposalRequest request, StreamObserver<NormalizeDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeDisposal(request));
    }

    /**
     * Scores a disposal.
     */
    @Override
    public void scoreDisposal(ScoreDisposalRequest request, StreamObserver<ScoreDisposalResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreDisposal(request));
    }

    /**
     * Lints a disposal.
     */
    @Override
    public void lintDisposals(LintDisposalsRequest request, StreamObserver<LintDisposalsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintDisposals(request));
    }

    /**
     * Returns a disposal.
     */
    @Override
    public void getDisposalRules(GetDisposalRulesRequest request, StreamObserver<GetDisposalRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getDisposalRules(request));
    }
}

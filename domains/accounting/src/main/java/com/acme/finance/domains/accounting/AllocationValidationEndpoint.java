package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.ExplainAllocationDecisionRequest;
import com.acme.accounting.v1.ExplainAllocationDecisionResponse;
import com.acme.accounting.v1.GetAllocationRulesRequest;
import com.acme.accounting.v1.GetAllocationRulesResponse;
import com.acme.accounting.v1.LintAllocationsRequest;
import com.acme.accounting.v1.LintAllocationsResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.ScoreAllocationRequest;
import com.acme.accounting.v1.ScoreAllocationResponse;
import com.acme.accounting.v1.ValidateAllocationRequest;
import com.acme.accounting.v1.ValidateAllocationResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Checks allocations against the rules before anything acts on them.
 */
@GrpcService
public class AllocationValidationEndpoint extends AllocationValidationServiceGrpc.AllocationValidationServiceImplBase {
    protected final AllocationValidationApplication application;

    public AllocationValidationEndpoint(AllocationValidationApplication application) {
        this.application = application;
    }

    /**
     * Validates an allocation.
     */
    @Override
    public void validateAllocation(ValidateAllocationRequest request, StreamObserver<ValidateAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.validateAllocation(request));
    }

    /**
     * Previews an allocation.
     */
    @Override
    public void previewAllocation(PreviewAllocationRequest request, StreamObserver<PreviewAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.previewAllocation(request));
    }

    /**
     * Normalizes an allocation.
     */
    @Override
    public void normalizeAllocation(NormalizeAllocationRequest request, StreamObserver<NormalizeAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.normalizeAllocation(request));
    }

    /**
     * Explains an allocation.
     */
    @Override
    public void explainAllocationDecision(ExplainAllocationDecisionRequest request, StreamObserver<ExplainAllocationDecisionResponse> observer) {
        RpcSupport.unary(observer, () -> application.explainAllocationDecision(request));
    }

    /**
     * Scores an allocation.
     */
    @Override
    public void scoreAllocation(ScoreAllocationRequest request, StreamObserver<ScoreAllocationResponse> observer) {
        RpcSupport.unary(observer, () -> application.scoreAllocation(request));
    }

    /**
     * Lints an allocation.
     */
    @Override
    public void lintAllocations(LintAllocationsRequest request, StreamObserver<LintAllocationsResponse> observer) {
        RpcSupport.unary(observer, () -> application.lintAllocations(request));
    }

    /**
     * Returns an allocation.
     */
    @Override
    public void getAllocationRules(GetAllocationRulesRequest request, StreamObserver<GetAllocationRulesResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAllocationRules(request));
    }
}

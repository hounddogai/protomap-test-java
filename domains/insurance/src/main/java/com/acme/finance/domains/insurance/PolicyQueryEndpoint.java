package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.BrowsePoliciesRequest;
import com.acme.insurance.v1.BrowsePoliciesResponse;
import com.acme.insurance.v1.CountPoliciesRequest;
import com.acme.insurance.v1.CountPoliciesResponse;
import com.acme.insurance.v1.ExportPoliciesRequest;
import com.acme.insurance.v1.ExportPoliciesResponse;
import com.acme.insurance.v1.FindPoliciesRequest;
import com.acme.insurance.v1.FindPoliciesResponse;
import com.acme.insurance.v1.LookupPolicyRequest;
import com.acme.insurance.v1.LookupPolicyResponse;
import com.acme.insurance.v1.PeekPolicyRequest;
import com.acme.insurance.v1.PeekPolicyResponse;
import com.acme.insurance.v1.PolicyQueryServiceGrpc;
import com.acme.insurance.v1.ResolvePoliciesRequest;
import com.acme.insurance.v1.ResolvePoliciesResponse;
import com.acme.insurance.v1.SuggestPoliciesRequest;
import com.acme.insurance.v1.SuggestPoliciesResponse;
import com.acme.insurance.v1.SummarizePoliciesRequest;
import com.acme.insurance.v1.SummarizePoliciesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about policies, for screens and reports.
 */
@GrpcService
public class PolicyQueryEndpoint extends PolicyQueryServiceGrpc.PolicyQueryServiceImplBase {
    protected final PolicyQueryApplication application;

    public PolicyQueryEndpoint(PolicyQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a policy.
     */
    @Override
    public void lookupPolicy(LookupPolicyRequest request, StreamObserver<LookupPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupPolicy(request));
    }

    /**
     * Lists policies a page at a time.
     */
    @Override
    public void browsePolicies(BrowsePoliciesRequest request, StreamObserver<BrowsePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browsePolicies(request));
    }

    /**
     * Lists policies a page at a time.
     */
    @Override
    public void findPolicies(FindPoliciesRequest request, StreamObserver<FindPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findPolicies(request));
    }

    /**
     * Counts policies by status.
     */
    @Override
    public void countPolicies(CountPoliciesRequest request, StreamObserver<CountPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countPolicies(request));
    }

    /**
     * Returns several policies at once.
     */
    @Override
    public void resolvePolicies(ResolvePoliciesRequest request, StreamObserver<ResolvePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolvePolicies(request));
    }

    /**
     * Streams every matching policy.
     */
    @Override
    public void exportPolicies(ExportPoliciesRequest request, StreamObserver<ExportPoliciesResponse> observer) {
        try {
            var snapshot = application.exportPolicies(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a policy.
     */
    @Override
    public void summarizePolicies(SummarizePoliciesRequest request, StreamObserver<SummarizePoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizePolicies(request));
    }

    /**
     * Lists policies a page at a time.
     */
    @Override
    public void suggestPolicies(SuggestPoliciesRequest request, StreamObserver<SuggestPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestPolicies(request));
    }

    /**
     * Returns a policy.
     */
    @Override
    public void peekPolicy(PeekPolicyRequest request, StreamObserver<PeekPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekPolicy(request));
    }
}

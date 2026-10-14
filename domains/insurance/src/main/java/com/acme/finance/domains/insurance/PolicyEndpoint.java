package com.acme.finance.domains.insurance;

import com.acme.finance.platform.RpcSupport;
import com.acme.insurance.v1.CreatePolicyRequest;
import com.acme.insurance.v1.CreatePolicyResponse;
import com.acme.insurance.v1.DeletePolicyRequest;
import com.acme.insurance.v1.DeletePolicyResponse;
import com.acme.insurance.v1.GetPolicyRequest;
import com.acme.insurance.v1.GetPolicyResponse;
import com.acme.insurance.v1.ListPoliciesRequest;
import com.acme.insurance.v1.ListPoliciesResponse;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.SearchPoliciesRequest;
import com.acme.insurance.v1.SearchPoliciesResponse;
import com.acme.insurance.v1.UpdatePolicyRequest;
import com.acme.insurance.v1.UpdatePolicyResponse;
import com.acme.insurance.v1.WatchPoliciesRequest;
import com.acme.insurance.v1.WatchPoliciesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Manages policies: creates, reads, changes, and lists them.
 */
@GrpcService
public class PolicyEndpoint extends PolicyServiceGrpc.PolicyServiceImplBase {
    protected final PolicyApplication application;

    public PolicyEndpoint(PolicyApplication application) {
        this.application = application;
    }

    /**
     * Creates a policy.
     */
    @Override
    public void createPolicy(CreatePolicyRequest request, StreamObserver<CreatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.createPolicy(request));
    }

    /**
     * Returns a policy.
     */
    @Override
    public void getPolicy(GetPolicyRequest request, StreamObserver<GetPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.getPolicy(request));
    }

    /**
     * Changes a policy.
     */
    @Override
    public void updatePolicy(UpdatePolicyRequest request, StreamObserver<UpdatePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.updatePolicy(request));
    }

    /**
     * Deletes a policy.
     */
    @Override
    public void deletePolicy(DeletePolicyRequest request, StreamObserver<DeletePolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.deletePolicy(request));
    }

    /**
     * Lists policies a page at a time.
     */
    @Override
    public void listPolicies(ListPoliciesRequest request, StreamObserver<ListPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.listPolicies(request));
    }

    /**
     * Lists policies a page at a time.
     */
    @Override
    public void searchPolicies(SearchPoliciesRequest request, StreamObserver<SearchPoliciesResponse> observer) {
        RpcSupport.unary(observer, () -> application.searchPolicies(request));
    }

    /**
     * Streams the changes of policies as they happen.
     */
    @Override
    public void watchPolicies(WatchPoliciesRequest request, StreamObserver<WatchPoliciesResponse> observer) {
        try {
            var snapshot = application.watchPolicies(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }
}

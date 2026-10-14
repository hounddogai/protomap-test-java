package com.acme.finance.domains.audit;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.GetRetentionPolicyVersionRequest;
import com.acme.audit.v1.GetRetentionPolicyVersionResponse;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.PruneRetentionPolicyVersionsRequest;
import com.acme.audit.v1.PruneRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps every version of a retention policy and restores earlier ones.
 */
@GrpcService
public class RetentionPolicyHistoryEndpoint extends RetentionPolicyHistoryServiceGrpc.RetentionPolicyHistoryServiceImplBase {
    protected final RetentionPolicyHistoryApplication application;

    public RetentionPolicyHistoryEndpoint(RetentionPolicyHistoryApplication application) {
        this.application = application;
    }

    /**
     * Records a retention policy.
     */
    @Override
    public void recordRetentionPolicyVersion(RecordRetentionPolicyVersionRequest request, StreamObserver<RecordRetentionPolicyVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recordRetentionPolicyVersion(request));
    }

    /**
     * Lists retention policies a page at a time.
     */
    @Override
    public void listRetentionPolicyVersions(ListRetentionPolicyVersionsRequest request, StreamObserver<ListRetentionPolicyVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listRetentionPolicyVersions(request));
    }

    /**
     * Returns a retention policy.
     */
    @Override
    public void getRetentionPolicyVersion(GetRetentionPolicyVersionRequest request, StreamObserver<GetRetentionPolicyVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.getRetentionPolicyVersion(request));
    }

    /**
     * Reverts a retention policy.
     */
    @Override
    public void revertRetentionPolicy(RevertRetentionPolicyRequest request, StreamObserver<RevertRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.revertRetentionPolicy(request));
    }

    /**
     * Annotates a retention policy.
     */
    @Override
    public void annotateRetentionPolicyVersion(AnnotateRetentionPolicyVersionRequest request, StreamObserver<AnnotateRetentionPolicyVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.annotateRetentionPolicyVersion(request));
    }

    /**
     * Deletes a retention policy.
     */
    @Override
    public void pruneRetentionPolicyVersions(PruneRetentionPolicyVersionsRequest request, StreamObserver<PruneRetentionPolicyVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pruneRetentionPolicyVersions(request));
    }
}

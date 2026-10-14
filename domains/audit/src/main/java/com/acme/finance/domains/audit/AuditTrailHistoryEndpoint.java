package com.acme.finance.domains.audit;

import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.PruneAuditTrailVersionsRequest;
import com.acme.audit.v1.PruneAuditTrailVersionsResponse;
import com.acme.audit.v1.RecordAuditTrailVersionRequest;
import com.acme.audit.v1.RecordAuditTrailVersionResponse;
import com.acme.audit.v1.RevertAuditTrailRequest;
import com.acme.audit.v1.RevertAuditTrailResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps every version of an audit trail and restores earlier ones.
 */
@GrpcService
public class AuditTrailHistoryEndpoint extends AuditTrailHistoryServiceGrpc.AuditTrailHistoryServiceImplBase {
    protected final AuditTrailHistoryApplication application;

    public AuditTrailHistoryEndpoint(AuditTrailHistoryApplication application) {
        this.application = application;
    }

    /**
     * Records an audit trail.
     */
    @Override
    public void recordAuditTrailVersion(RecordAuditTrailVersionRequest request, StreamObserver<RecordAuditTrailVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recordAuditTrailVersion(request));
    }

    /**
     * Lists audit trails a page at a time.
     */
    @Override
    public void listAuditTrailVersions(ListAuditTrailVersionsRequest request, StreamObserver<ListAuditTrailVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAuditTrailVersions(request));
    }

    /**
     * Compares an audit trail.
     */
    @Override
    public void compareAuditTrailVersions(CompareAuditTrailVersionsRequest request, StreamObserver<CompareAuditTrailVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareAuditTrailVersions(request));
    }

    /**
     * Reverts an audit trail.
     */
    @Override
    public void revertAuditTrail(RevertAuditTrailRequest request, StreamObserver<RevertAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.revertAuditTrail(request));
    }

    /**
     * Annotates an audit trail.
     */
    @Override
    public void annotateAuditTrailVersion(AnnotateAuditTrailVersionRequest request, StreamObserver<AnnotateAuditTrailVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.annotateAuditTrailVersion(request));
    }

    /**
     * Deletes an audit trail.
     */
    @Override
    public void pruneAuditTrailVersions(PruneAuditTrailVersionsRequest request, StreamObserver<PruneAuditTrailVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pruneAuditTrailVersions(request));
    }
}

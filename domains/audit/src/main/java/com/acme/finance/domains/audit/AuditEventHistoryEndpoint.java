package com.acme.finance.domains.audit;

import com.acme.audit.v1.AnnotateAuditEventVersionRequest;
import com.acme.audit.v1.AnnotateAuditEventVersionResponse;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditEventVersionsRequest;
import com.acme.audit.v1.CompareAuditEventVersionsResponse;
import com.acme.audit.v1.GetAuditEventVersionRequest;
import com.acme.audit.v1.GetAuditEventVersionResponse;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.audit.v1.PruneAuditEventVersionsRequest;
import com.acme.audit.v1.PruneAuditEventVersionsResponse;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps every version of an audit event and restores earlier ones.
 */
@GrpcService
public class AuditEventHistoryEndpoint extends AuditEventHistoryServiceGrpc.AuditEventHistoryServiceImplBase {
    protected final AuditEventHistoryApplication application;

    public AuditEventHistoryEndpoint(AuditEventHistoryApplication application) {
        this.application = application;
    }

    /**
     * Records an audit event.
     */
    @Override
    public void recordAuditEventVersion(RecordAuditEventVersionRequest request, StreamObserver<RecordAuditEventVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recordAuditEventVersion(request));
    }

    /**
     * Lists audit events a page at a time.
     */
    @Override
    public void listAuditEventVersions(ListAuditEventVersionsRequest request, StreamObserver<ListAuditEventVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAuditEventVersions(request));
    }

    /**
     * Returns an audit event.
     */
    @Override
    public void getAuditEventVersion(GetAuditEventVersionRequest request, StreamObserver<GetAuditEventVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditEventVersion(request));
    }

    /**
     * Compares an audit event.
     */
    @Override
    public void compareAuditEventVersions(CompareAuditEventVersionsRequest request, StreamObserver<CompareAuditEventVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareAuditEventVersions(request));
    }

    /**
     * Reverts an audit event.
     */
    @Override
    public void revertAuditEvent(RevertAuditEventRequest request, StreamObserver<RevertAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.revertAuditEvent(request));
    }

    /**
     * Annotates an audit event.
     */
    @Override
    public void annotateAuditEventVersion(AnnotateAuditEventVersionRequest request, StreamObserver<AnnotateAuditEventVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.annotateAuditEventVersion(request));
    }

    /**
     * Deletes an audit event.
     */
    @Override
    public void pruneAuditEventVersions(PruneAuditEventVersionsRequest request, StreamObserver<PruneAuditEventVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pruneAuditEventVersions(request));
    }
}

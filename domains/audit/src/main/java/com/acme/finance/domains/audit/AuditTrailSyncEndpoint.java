package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.GetAuditTrailSyncStateRequest;
import com.acme.audit.v1.GetAuditTrailSyncStateResponse;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.audit.v1.ReconcileAuditTrailsRequest;
import com.acme.audit.v1.ReconcileAuditTrailsResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.SyncAuditTrailsRequest;
import com.acme.audit.v1.SyncAuditTrailsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps audit trails in step with the systems of record in other regions.
 */
@GrpcService
public class AuditTrailSyncEndpoint extends AuditTrailSyncServiceGrpc.AuditTrailSyncServiceImplBase {
    protected final AuditTrailSyncApplication application;

    public AuditTrailSyncEndpoint(AuditTrailSyncApplication application) {
        this.application = application;
    }

    /**
     * Syncs an audit trail.
     */
    @Override
    public void syncAuditTrails(SyncAuditTrailsRequest request, StreamObserver<SyncAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.syncAuditTrails(request));
    }

    /**
     * Pushs an audit trail.
     */
    @Override
    public void pushAuditTrail(PushAuditTrailRequest request, StreamObserver<PushAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.pushAuditTrail(request));
    }

    /**
     * Pulls an audit trail.
     */
    @Override
    public void pullAuditTrails(PullAuditTrailsRequest request, StreamObserver<PullAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pullAuditTrails(request));
    }

    /**
     * Reconciles an audit trail.
     */
    @Override
    public void reconcileAuditTrails(ReconcileAuditTrailsRequest request, StreamObserver<ReconcileAuditTrailsResponse> observer) {
        RpcSupport.unary(observer, () -> application.reconcileAuditTrails(request));
    }

    /**
     * Resolves an audit trail.
     */
    @Override
    public void resolveAuditTrailConflict(ResolveAuditTrailConflictRequest request, StreamObserver<ResolveAuditTrailConflictResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveAuditTrailConflict(request));
    }

    /**
     * Returns an audit trail.
     */
    @Override
    public void getAuditTrailSyncState(GetAuditTrailSyncStateRequest request, StreamObserver<GetAuditTrailSyncStateResponse> observer) {
        RpcSupport.unary(observer, () -> application.getAuditTrailSyncState(request));
    }

    /**
     * Lists audit trails a page at a time.
     */
    @Override
    public void listAuditTrailConflicts(ListAuditTrailConflictsRequest request, StreamObserver<ListAuditTrailConflictsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listAuditTrailConflicts(request));
    }
}

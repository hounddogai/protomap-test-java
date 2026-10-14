package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveAuditTrailRequest;
import com.acme.audit.v1.ArchiveAuditTrailResponse;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.InspectAuditTrailRequest;
import com.acme.audit.v1.InspectAuditTrailResponse;
import com.acme.audit.v1.LockAuditTrailRequest;
import com.acme.audit.v1.LockAuditTrailResponse;
import com.acme.audit.v1.PurgeAuditTrailRequest;
import com.acme.audit.v1.PurgeAuditTrailResponse;
import com.acme.audit.v1.ReassignAuditTrailRequest;
import com.acme.audit.v1.ReassignAuditTrailResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.audit.v1.RestoreAuditTrailRequest;
import com.acme.audit.v1.RestoreAuditTrailResponse;
import com.acme.audit.v1.TransferAuditTrailRequest;
import com.acme.audit.v1.TransferAuditTrailResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Operator actions on audit trails that support staff run by hand.
 */
@GrpcService
public class AuditTrailAdminEndpoint extends AuditTrailAdminServiceGrpc.AuditTrailAdminServiceImplBase {
    protected final AuditTrailAdminApplication application;

    public AuditTrailAdminEndpoint(AuditTrailAdminApplication application) {
        this.application = application;
    }

    /**
     * Archives an audit trail.
     */
    @Override
    public void archiveAuditTrail(ArchiveAuditTrailRequest request, StreamObserver<ArchiveAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.archiveAuditTrail(request));
    }

    /**
     * Restores an audit trail.
     */
    @Override
    public void restoreAuditTrail(RestoreAuditTrailRequest request, StreamObserver<RestoreAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.restoreAuditTrail(request));
    }

    /**
     * Deletes an audit trail.
     */
    @Override
    public void purgeAuditTrail(PurgeAuditTrailRequest request, StreamObserver<PurgeAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.purgeAuditTrail(request));
    }

    /**
     * Reassigns an audit trail.
     */
    @Override
    public void reassignAuditTrail(ReassignAuditTrailRequest request, StreamObserver<ReassignAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.reassignAuditTrail(request));
    }

    /**
     * Locks an audit trail.
     */
    @Override
    public void lockAuditTrail(LockAuditTrailRequest request, StreamObserver<LockAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.lockAuditTrail(request));
    }

    /**
     * Transfers an audit trail.
     */
    @Override
    public void transferAuditTrail(TransferAuditTrailRequest request, StreamObserver<TransferAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.transferAuditTrail(request));
    }

    /**
     * Repairs an audit trail.
     */
    @Override
    public void repairAuditTrail(RepairAuditTrailRequest request, StreamObserver<RepairAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.repairAuditTrail(request));
    }

    /**
     * Returns an audit trail.
     */
    @Override
    public void inspectAuditTrail(InspectAuditTrailRequest request, StreamObserver<InspectAuditTrailResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectAuditTrail(request));
    }
}

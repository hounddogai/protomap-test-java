package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveAuditEventRequest;
import com.acme.audit.v1.ArchiveAuditEventResponse;
import com.acme.audit.v1.AuditEventAdminServiceGrpc;
import com.acme.audit.v1.InspectAuditEventRequest;
import com.acme.audit.v1.InspectAuditEventResponse;
import com.acme.audit.v1.LockAuditEventRequest;
import com.acme.audit.v1.LockAuditEventResponse;
import com.acme.audit.v1.PurgeAuditEventRequest;
import com.acme.audit.v1.PurgeAuditEventResponse;
import com.acme.audit.v1.ReassignAuditEventRequest;
import com.acme.audit.v1.ReassignAuditEventResponse;
import com.acme.audit.v1.RestoreAuditEventRequest;
import com.acme.audit.v1.RestoreAuditEventResponse;
import com.acme.audit.v1.TransferAuditEventRequest;
import com.acme.audit.v1.TransferAuditEventResponse;
import com.acme.audit.v1.UnlockAuditEventRequest;
import com.acme.audit.v1.UnlockAuditEventResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Operator actions on audit events that support staff run by hand.
 */
@GrpcService
public class AuditEventAdminEndpoint extends AuditEventAdminServiceGrpc.AuditEventAdminServiceImplBase {
    protected final AuditEventAdminApplication application;

    public AuditEventAdminEndpoint(AuditEventAdminApplication application) {
        this.application = application;
    }

    /**
     * Archives an audit event.
     */
    @Override
    public void archiveAuditEvent(ArchiveAuditEventRequest request, StreamObserver<ArchiveAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.archiveAuditEvent(request));
    }

    /**
     * Restores an audit event.
     */
    @Override
    public void restoreAuditEvent(RestoreAuditEventRequest request, StreamObserver<RestoreAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.restoreAuditEvent(request));
    }

    /**
     * Deletes an audit event.
     */
    @Override
    public void purgeAuditEvent(PurgeAuditEventRequest request, StreamObserver<PurgeAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.purgeAuditEvent(request));
    }

    /**
     * Reassigns an audit event.
     */
    @Override
    public void reassignAuditEvent(ReassignAuditEventRequest request, StreamObserver<ReassignAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.reassignAuditEvent(request));
    }

    /**
     * Locks an audit event.
     */
    @Override
    public void lockAuditEvent(LockAuditEventRequest request, StreamObserver<LockAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.lockAuditEvent(request));
    }

    /**
     * Unlocks an audit event.
     */
    @Override
    public void unlockAuditEvent(UnlockAuditEventRequest request, StreamObserver<UnlockAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.unlockAuditEvent(request));
    }

    /**
     * Transfers an audit event.
     */
    @Override
    public void transferAuditEvent(TransferAuditEventRequest request, StreamObserver<TransferAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.transferAuditEvent(request));
    }

    /**
     * Returns an audit event.
     */
    @Override
    public void inspectAuditEvent(InspectAuditEventRequest request, StreamObserver<InspectAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectAuditEvent(request));
    }
}

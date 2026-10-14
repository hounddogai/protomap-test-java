package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveRetentionPolicyRequest;
import com.acme.audit.v1.ArchiveRetentionPolicyResponse;
import com.acme.audit.v1.PurgeRetentionPolicyRequest;
import com.acme.audit.v1.PurgeRetentionPolicyResponse;
import com.acme.audit.v1.RepairRetentionPolicyRequest;
import com.acme.audit.v1.RepairRetentionPolicyResponse;
import com.acme.audit.v1.RestoreRetentionPolicyRequest;
import com.acme.audit.v1.RestoreRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicyAdminServiceGrpc;
import com.acme.audit.v1.TransferRetentionPolicyRequest;
import com.acme.audit.v1.TransferRetentionPolicyResponse;
import com.acme.audit.v1.UnlockRetentionPolicyRequest;
import com.acme.audit.v1.UnlockRetentionPolicyResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;

/**
 * Operator actions on retention policies that support staff run by hand.
 */
public abstract class AbstractRetentionPolicyAdminEndpoint extends RetentionPolicyAdminServiceGrpc.RetentionPolicyAdminServiceImplBase {
    protected final RetentionPolicyAdminApplication application;

    protected AbstractRetentionPolicyAdminEndpoint(RetentionPolicyAdminApplication application) {
        this.application = application;
    }

    /**
     * Archives a retention policy.
     */
    @Override
    public void archiveRetentionPolicy(ArchiveRetentionPolicyRequest request, StreamObserver<ArchiveRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.archiveRetentionPolicy(request));
    }

    /**
     * Restores a retention policy.
     */
    @Override
    public void restoreRetentionPolicy(RestoreRetentionPolicyRequest request, StreamObserver<RestoreRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.restoreRetentionPolicy(request));
    }

    /**
     * Deletes a retention policy.
     */
    @Override
    public void purgeRetentionPolicy(PurgeRetentionPolicyRequest request, StreamObserver<PurgeRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.purgeRetentionPolicy(request));
    }

    /**
     * Unlocks a retention policy.
     */
    @Override
    public void unlockRetentionPolicy(UnlockRetentionPolicyRequest request, StreamObserver<UnlockRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.unlockRetentionPolicy(request));
    }

    /**
     * Transfers a retention policy.
     */
    @Override
    public void transferRetentionPolicy(TransferRetentionPolicyRequest request, StreamObserver<TransferRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.transferRetentionPolicy(request));
    }

    /**
     * Repairs a retention policy.
     */
    @Override
    public void repairRetentionPolicy(RepairRetentionPolicyRequest request, StreamObserver<RepairRetentionPolicyResponse> observer) {
        RpcSupport.unary(observer, () -> application.repairRetentionPolicy(request));
    }
}

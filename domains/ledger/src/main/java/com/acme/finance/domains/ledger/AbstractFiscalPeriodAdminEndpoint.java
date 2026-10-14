package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.ArchiveFiscalPeriodRequest;
import com.acme.ledger.v1.ArchiveFiscalPeriodResponse;
import com.acme.ledger.v1.FiscalPeriodAdminServiceGrpc;
import com.acme.ledger.v1.InspectFiscalPeriodRequest;
import com.acme.ledger.v1.InspectFiscalPeriodResponse;
import com.acme.ledger.v1.LockFiscalPeriodRequest;
import com.acme.ledger.v1.LockFiscalPeriodResponse;
import com.acme.ledger.v1.PurgeFiscalPeriodRequest;
import com.acme.ledger.v1.PurgeFiscalPeriodResponse;
import com.acme.ledger.v1.ReassignFiscalPeriodRequest;
import com.acme.ledger.v1.ReassignFiscalPeriodResponse;
import com.acme.ledger.v1.RepairFiscalPeriodRequest;
import com.acme.ledger.v1.RepairFiscalPeriodResponse;
import com.acme.ledger.v1.RestoreFiscalPeriodRequest;
import com.acme.ledger.v1.RestoreFiscalPeriodResponse;
import com.acme.ledger.v1.TransferFiscalPeriodRequest;
import com.acme.ledger.v1.TransferFiscalPeriodResponse;
import com.acme.ledger.v1.UnlockFiscalPeriodRequest;
import com.acme.ledger.v1.UnlockFiscalPeriodResponse;
import io.grpc.stub.StreamObserver;

/**
 * Operator actions on fiscal periods that support staff run by hand.
 */
public abstract class AbstractFiscalPeriodAdminEndpoint extends FiscalPeriodAdminServiceGrpc.FiscalPeriodAdminServiceImplBase {
    protected final FiscalPeriodAdminApplication application;

    protected AbstractFiscalPeriodAdminEndpoint(FiscalPeriodAdminApplication application) {
        this.application = application;
    }

    /**
     * Archives a fiscal period.
     */
    @Override
    public void archiveFiscalPeriod(ArchiveFiscalPeriodRequest request, StreamObserver<ArchiveFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.archiveFiscalPeriod(request));
    }

    /**
     * Restores a fiscal period.
     */
    @Override
    public void restoreFiscalPeriod(RestoreFiscalPeriodRequest request, StreamObserver<RestoreFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.restoreFiscalPeriod(request));
    }

    /**
     * Deletes a fiscal period.
     */
    @Override
    public void purgeFiscalPeriod(PurgeFiscalPeriodRequest request, StreamObserver<PurgeFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.purgeFiscalPeriod(request));
    }

    /**
     * Reassigns a fiscal period.
     */
    @Override
    public void reassignFiscalPeriod(ReassignFiscalPeriodRequest request, StreamObserver<ReassignFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.reassignFiscalPeriod(request));
    }

    /**
     * Locks a fiscal period.
     */
    @Override
    public void lockFiscalPeriod(LockFiscalPeriodRequest request, StreamObserver<LockFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.lockFiscalPeriod(request));
    }

    /**
     * Unlocks a fiscal period.
     */
    @Override
    public void unlockFiscalPeriod(UnlockFiscalPeriodRequest request, StreamObserver<UnlockFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.unlockFiscalPeriod(request));
    }

    /**
     * Transfers a fiscal period.
     */
    @Override
    public void transferFiscalPeriod(TransferFiscalPeriodRequest request, StreamObserver<TransferFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.transferFiscalPeriod(request));
    }

    /**
     * Repairs a fiscal period.
     */
    @Override
    public void repairFiscalPeriod(RepairFiscalPeriodRequest request, StreamObserver<RepairFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.repairFiscalPeriod(request));
    }

    /**
     * Returns a fiscal period.
     */
    @Override
    public void inspectFiscalPeriod(InspectFiscalPeriodRequest request, StreamObserver<InspectFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.inspectFiscalPeriod(request));
    }
}

package com.acme.finance.domains.ledger;

import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.GetFiscalPeriodVersionRequest;
import com.acme.ledger.v1.GetFiscalPeriodVersionResponse;
import com.acme.ledger.v1.ListFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.PruneFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.PruneFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Keeps every version of a fiscal period and restores earlier ones.
 */
@GrpcService
public class FiscalPeriodHistoryEndpoint extends FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceImplBase {
    protected final FiscalPeriodHistoryApplication application;

    public FiscalPeriodHistoryEndpoint(FiscalPeriodHistoryApplication application) {
        this.application = application;
    }

    /**
     * Records a fiscal period.
     */
    @Override
    public void recordFiscalPeriodVersion(RecordFiscalPeriodVersionRequest request, StreamObserver<RecordFiscalPeriodVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.recordFiscalPeriodVersion(request));
    }

    /**
     * Lists fiscal periods a page at a time.
     */
    @Override
    public void listFiscalPeriodVersions(ListFiscalPeriodVersionsRequest request, StreamObserver<ListFiscalPeriodVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.listFiscalPeriodVersions(request));
    }

    /**
     * Returns a fiscal period.
     */
    @Override
    public void getFiscalPeriodVersion(GetFiscalPeriodVersionRequest request, StreamObserver<GetFiscalPeriodVersionResponse> observer) {
        RpcSupport.unary(observer, () -> application.getFiscalPeriodVersion(request));
    }

    /**
     * Compares a fiscal period.
     */
    @Override
    public void compareFiscalPeriodVersions(CompareFiscalPeriodVersionsRequest request, StreamObserver<CompareFiscalPeriodVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.compareFiscalPeriodVersions(request));
    }

    /**
     * Reverts a fiscal period.
     */
    @Override
    public void revertFiscalPeriod(RevertFiscalPeriodRequest request, StreamObserver<RevertFiscalPeriodResponse> observer) {
        RpcSupport.unary(observer, () -> application.revertFiscalPeriod(request));
    }

    /**
     * Deletes a fiscal period.
     */
    @Override
    public void pruneFiscalPeriodVersions(PruneFiscalPeriodVersionsRequest request, StreamObserver<PruneFiscalPeriodVersionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.pruneFiscalPeriodVersions(request));
    }
}

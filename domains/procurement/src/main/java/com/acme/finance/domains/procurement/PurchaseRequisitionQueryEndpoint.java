package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.BrowsePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BrowsePurchaseRequisitionsResponse;
import com.acme.procurement.v1.CountPurchaseRequisitionsRequest;
import com.acme.procurement.v1.CountPurchaseRequisitionsResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.FindPurchaseRequisitionsRequest;
import com.acme.procurement.v1.FindPurchaseRequisitionsResponse;
import com.acme.procurement.v1.LookupPurchaseRequisitionRequest;
import com.acme.procurement.v1.LookupPurchaseRequisitionResponse;
import com.acme.procurement.v1.PeekPurchaseRequisitionRequest;
import com.acme.procurement.v1.PeekPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsRequest;
import com.acme.procurement.v1.ResolvePurchaseRequisitionsResponse;
import com.acme.procurement.v1.SuggestPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SuggestPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsRequest;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about purchase requisitions, for screens and reports.
 */
@GrpcService
public class PurchaseRequisitionQueryEndpoint extends PurchaseRequisitionQueryServiceGrpc.PurchaseRequisitionQueryServiceImplBase {
    protected final PurchaseRequisitionQueryApplication application;

    public PurchaseRequisitionQueryEndpoint(PurchaseRequisitionQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a purchase requisition.
     */
    @Override
    public void lookupPurchaseRequisition(LookupPurchaseRequisitionRequest request, StreamObserver<LookupPurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupPurchaseRequisition(request));
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    @Override
    public void browsePurchaseRequisitions(BrowsePurchaseRequisitionsRequest request, StreamObserver<BrowsePurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browsePurchaseRequisitions(request));
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    @Override
    public void findPurchaseRequisitions(FindPurchaseRequisitionsRequest request, StreamObserver<FindPurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findPurchaseRequisitions(request));
    }

    /**
     * Counts purchase requisitions by status.
     */
    @Override
    public void countPurchaseRequisitions(CountPurchaseRequisitionsRequest request, StreamObserver<CountPurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countPurchaseRequisitions(request));
    }

    /**
     * Returns several purchase requisitions at once.
     */
    @Override
    public void resolvePurchaseRequisitions(ResolvePurchaseRequisitionsRequest request, StreamObserver<ResolvePurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolvePurchaseRequisitions(request));
    }

    /**
     * Streams every matching purchase requisition.
     */
    @Override
    public void exportPurchaseRequisitions(ExportPurchaseRequisitionsRequest request, StreamObserver<ExportPurchaseRequisitionsResponse> observer) {
        try {
            var snapshot = application.exportPurchaseRequisitions(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a purchase requisition.
     */
    @Override
    public void summarizePurchaseRequisitions(SummarizePurchaseRequisitionsRequest request, StreamObserver<SummarizePurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizePurchaseRequisitions(request));
    }

    /**
     * Lists purchase requisitions a page at a time.
     */
    @Override
    public void suggestPurchaseRequisitions(SuggestPurchaseRequisitionsRequest request, StreamObserver<SuggestPurchaseRequisitionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestPurchaseRequisitions(request));
    }

    /**
     * Returns a purchase requisition.
     */
    @Override
    public void peekPurchaseRequisition(PeekPurchaseRequisitionRequest request, StreamObserver<PeekPurchaseRequisitionResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekPurchaseRequisition(request));
    }
}

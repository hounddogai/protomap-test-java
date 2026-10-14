package com.acme.finance.domains.procurement;

import com.acme.finance.platform.RpcSupport;
import com.acme.procurement.v1.BrowsePurchaseOrdersRequest;
import com.acme.procurement.v1.BrowsePurchaseOrdersResponse;
import com.acme.procurement.v1.CountPurchaseOrdersRequest;
import com.acme.procurement.v1.CountPurchaseOrdersResponse;
import com.acme.procurement.v1.ExportPurchaseOrdersRequest;
import com.acme.procurement.v1.ExportPurchaseOrdersResponse;
import com.acme.procurement.v1.FindPurchaseOrdersRequest;
import com.acme.procurement.v1.FindPurchaseOrdersResponse;
import com.acme.procurement.v1.LookupPurchaseOrderRequest;
import com.acme.procurement.v1.LookupPurchaseOrderResponse;
import com.acme.procurement.v1.PeekPurchaseOrderRequest;
import com.acme.procurement.v1.PeekPurchaseOrderResponse;
import com.acme.procurement.v1.PurchaseOrderQueryServiceGrpc;
import com.acme.procurement.v1.ResolvePurchaseOrdersRequest;
import com.acme.procurement.v1.ResolvePurchaseOrdersResponse;
import com.acme.procurement.v1.SuggestPurchaseOrdersRequest;
import com.acme.procurement.v1.SuggestPurchaseOrdersResponse;
import com.acme.procurement.v1.SummarizePurchaseOrdersRequest;
import com.acme.procurement.v1.SummarizePurchaseOrdersResponse;
import io.grpc.stub.StreamObserver;

/**
 * Answers read-only questions about purchase orders, for screens and reports.
 */
public abstract class AbstractPurchaseOrderQueryEndpoint extends PurchaseOrderQueryServiceGrpc.PurchaseOrderQueryServiceImplBase {
    protected final PurchaseOrderQueryApplication application;

    protected AbstractPurchaseOrderQueryEndpoint(PurchaseOrderQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a purchase order.
     */
    @Override
    public void lookupPurchaseOrder(LookupPurchaseOrderRequest request, StreamObserver<LookupPurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupPurchaseOrder(request));
    }

    /**
     * Lists purchase orders a page at a time.
     */
    @Override
    public void browsePurchaseOrders(BrowsePurchaseOrdersRequest request, StreamObserver<BrowsePurchaseOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.browsePurchaseOrders(request));
    }

    /**
     * Lists purchase orders a page at a time.
     */
    @Override
    public void findPurchaseOrders(FindPurchaseOrdersRequest request, StreamObserver<FindPurchaseOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.findPurchaseOrders(request));
    }

    /**
     * Counts purchase orders by status.
     */
    @Override
    public void countPurchaseOrders(CountPurchaseOrdersRequest request, StreamObserver<CountPurchaseOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.countPurchaseOrders(request));
    }

    /**
     * Returns several purchase orders at once.
     */
    @Override
    public void resolvePurchaseOrders(ResolvePurchaseOrdersRequest request, StreamObserver<ResolvePurchaseOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolvePurchaseOrders(request));
    }

    /**
     * Streams every matching purchase order.
     */
    @Override
    public void exportPurchaseOrders(ExportPurchaseOrdersRequest request, StreamObserver<ExportPurchaseOrdersResponse> observer) {
        try {
            var snapshot = application.exportPurchaseOrders(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a purchase order.
     */
    @Override
    public void summarizePurchaseOrders(SummarizePurchaseOrdersRequest request, StreamObserver<SummarizePurchaseOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizePurchaseOrders(request));
    }

    /**
     * Lists purchase orders a page at a time.
     */
    @Override
    public void suggestPurchaseOrders(SuggestPurchaseOrdersRequest request, StreamObserver<SuggestPurchaseOrdersResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestPurchaseOrders(request));
    }

    /**
     * Returns a purchase order.
     */
    @Override
    public void peekPurchaseOrder(PeekPurchaseOrderRequest request, StreamObserver<PeekPurchaseOrderResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekPurchaseOrder(request));
    }
}

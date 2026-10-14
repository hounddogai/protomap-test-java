package com.acme.finance.domains.expenses;

import com.acme.expenses.v1.BrowseReceiptsRequest;
import com.acme.expenses.v1.BrowseReceiptsResponse;
import com.acme.expenses.v1.CountReceiptsRequest;
import com.acme.expenses.v1.CountReceiptsResponse;
import com.acme.expenses.v1.ExportReceiptsRequest;
import com.acme.expenses.v1.ExportReceiptsResponse;
import com.acme.expenses.v1.FindReceiptsRequest;
import com.acme.expenses.v1.FindReceiptsResponse;
import com.acme.expenses.v1.LookupReceiptRequest;
import com.acme.expenses.v1.LookupReceiptResponse;
import com.acme.expenses.v1.PeekReceiptRequest;
import com.acme.expenses.v1.PeekReceiptResponse;
import com.acme.expenses.v1.ReceiptQueryServiceGrpc;
import com.acme.expenses.v1.SuggestReceiptsRequest;
import com.acme.expenses.v1.SuggestReceiptsResponse;
import com.acme.expenses.v1.SummarizeReceiptsRequest;
import com.acme.expenses.v1.SummarizeReceiptsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about receipts, for screens and reports.
 */
@GrpcService
public class ReceiptQueryEndpoint extends ReceiptQueryServiceGrpc.ReceiptQueryServiceImplBase {
    protected final ReceiptQueryApplication application;

    public ReceiptQueryEndpoint(ReceiptQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a receipt.
     */
    @Override
    public void lookupReceipt(LookupReceiptRequest request, StreamObserver<LookupReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupReceipt(request));
    }

    /**
     * Lists receipts a page at a time.
     */
    @Override
    public void browseReceipts(BrowseReceiptsRequest request, StreamObserver<BrowseReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseReceipts(request));
    }

    /**
     * Lists receipts a page at a time.
     */
    @Override
    public void findReceipts(FindReceiptsRequest request, StreamObserver<FindReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findReceipts(request));
    }

    /**
     * Counts receipts by status.
     */
    @Override
    public void countReceipts(CountReceiptsRequest request, StreamObserver<CountReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countReceipts(request));
    }

    /**
     * Streams every matching receipt.
     */
    @Override
    public void exportReceipts(ExportReceiptsRequest request, StreamObserver<ExportReceiptsResponse> observer) {
        try {
            var snapshot = application.exportReceipts(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a receipt.
     */
    @Override
    public void summarizeReceipts(SummarizeReceiptsRequest request, StreamObserver<SummarizeReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeReceipts(request));
    }

    /**
     * Lists receipts a page at a time.
     */
    @Override
    public void suggestReceipts(SuggestReceiptsRequest request, StreamObserver<SuggestReceiptsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestReceipts(request));
    }

    /**
     * Returns a receipt.
     */
    @Override
    public void peekReceipt(PeekReceiptRequest request, StreamObserver<PeekReceiptResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekReceipt(request));
    }
}

package com.acme.finance.domains.treasury;

import com.acme.finance.platform.RpcSupport;
import com.acme.treasury.v1.BrowseCashPositionsRequest;
import com.acme.treasury.v1.BrowseCashPositionsResponse;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CountCashPositionsRequest;
import com.acme.treasury.v1.CountCashPositionsResponse;
import com.acme.treasury.v1.ExportCashPositionsRequest;
import com.acme.treasury.v1.ExportCashPositionsResponse;
import com.acme.treasury.v1.LookupCashPositionRequest;
import com.acme.treasury.v1.LookupCashPositionResponse;
import com.acme.treasury.v1.PeekCashPositionRequest;
import com.acme.treasury.v1.PeekCashPositionResponse;
import com.acme.treasury.v1.ResolveCashPositionsRequest;
import com.acme.treasury.v1.ResolveCashPositionsResponse;
import com.acme.treasury.v1.SuggestCashPositionsRequest;
import com.acme.treasury.v1.SuggestCashPositionsResponse;
import com.acme.treasury.v1.SummarizeCashPositionsRequest;
import com.acme.treasury.v1.SummarizeCashPositionsResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about cash positions, for screens and reports.
 */
@GrpcService
public class CashPositionQueryEndpoint extends CashPositionQueryServiceGrpc.CashPositionQueryServiceImplBase {
    protected final CashPositionQueryApplication application;

    public CashPositionQueryEndpoint(CashPositionQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a cash position.
     */
    @Override
    public void lookupCashPosition(LookupCashPositionRequest request, StreamObserver<LookupCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupCashPosition(request));
    }

    /**
     * Lists cash positions a page at a time.
     */
    @Override
    public void browseCashPositions(BrowseCashPositionsRequest request, StreamObserver<BrowseCashPositionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseCashPositions(request));
    }

    /**
     * Counts cash positions by status.
     */
    @Override
    public void countCashPositions(CountCashPositionsRequest request, StreamObserver<CountCashPositionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countCashPositions(request));
    }

    /**
     * Returns several cash positions at once.
     */
    @Override
    public void resolveCashPositions(ResolveCashPositionsRequest request, StreamObserver<ResolveCashPositionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveCashPositions(request));
    }

    /**
     * Streams every matching cash position.
     */
    @Override
    public void exportCashPositions(ExportCashPositionsRequest request, StreamObserver<ExportCashPositionsResponse> observer) {
        try {
            var snapshot = application.exportCashPositions(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a cash position.
     */
    @Override
    public void summarizeCashPositions(SummarizeCashPositionsRequest request, StreamObserver<SummarizeCashPositionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeCashPositions(request));
    }

    /**
     * Lists cash positions a page at a time.
     */
    @Override
    public void suggestCashPositions(SuggestCashPositionsRequest request, StreamObserver<SuggestCashPositionsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestCashPositions(request));
    }

    /**
     * Returns a cash position.
     */
    @Override
    public void peekCashPosition(PeekCashPositionRequest request, StreamObserver<PeekCashPositionResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekCashPosition(request));
    }
}

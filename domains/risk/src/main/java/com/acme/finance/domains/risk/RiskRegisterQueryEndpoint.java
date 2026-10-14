package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.BrowseRiskRegistersRequest;
import com.acme.risk.v1.BrowseRiskRegistersResponse;
import com.acme.risk.v1.ExportRiskRegistersRequest;
import com.acme.risk.v1.ExportRiskRegistersResponse;
import com.acme.risk.v1.FindRiskRegistersRequest;
import com.acme.risk.v1.FindRiskRegistersResponse;
import com.acme.risk.v1.LookupRiskRegisterRequest;
import com.acme.risk.v1.LookupRiskRegisterResponse;
import com.acme.risk.v1.PeekRiskRegisterRequest;
import com.acme.risk.v1.PeekRiskRegisterResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.SuggestRiskRegistersRequest;
import com.acme.risk.v1.SuggestRiskRegistersResponse;
import com.acme.risk.v1.SummarizeRiskRegistersRequest;
import com.acme.risk.v1.SummarizeRiskRegistersResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about risk registers, for screens and reports.
 */
@GrpcService
public class RiskRegisterQueryEndpoint extends RiskRegisterQueryServiceGrpc.RiskRegisterQueryServiceImplBase {
    protected final RiskRegisterQueryApplication application;

    public RiskRegisterQueryEndpoint(RiskRegisterQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a risk register.
     */
    @Override
    public void lookupRiskRegister(LookupRiskRegisterRequest request, StreamObserver<LookupRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupRiskRegister(request));
    }

    /**
     * Lists risk registers a page at a time.
     */
    @Override
    public void browseRiskRegisters(BrowseRiskRegistersRequest request, StreamObserver<BrowseRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseRiskRegisters(request));
    }

    /**
     * Lists risk registers a page at a time.
     */
    @Override
    public void findRiskRegisters(FindRiskRegistersRequest request, StreamObserver<FindRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.findRiskRegisters(request));
    }

    /**
     * Returns several risk registers at once.
     */
    @Override
    public void resolveRiskRegisters(ResolveRiskRegistersRequest request, StreamObserver<ResolveRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveRiskRegisters(request));
    }

    /**
     * Streams every matching risk register.
     */
    @Override
    public void exportRiskRegisters(ExportRiskRegistersRequest request, StreamObserver<ExportRiskRegistersResponse> observer) {
        try {
            var snapshot = application.exportRiskRegisters(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a risk register.
     */
    @Override
    public void summarizeRiskRegisters(SummarizeRiskRegistersRequest request, StreamObserver<SummarizeRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeRiskRegisters(request));
    }

    /**
     * Lists risk registers a page at a time.
     */
    @Override
    public void suggestRiskRegisters(SuggestRiskRegistersRequest request, StreamObserver<SuggestRiskRegistersResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestRiskRegisters(request));
    }

    /**
     * Returns a risk register.
     */
    @Override
    public void peekRiskRegister(PeekRiskRegisterRequest request, StreamObserver<PeekRiskRegisterResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekRiskRegister(request));
    }
}

package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.BrowseControlsRequest;
import com.acme.compliance.v1.BrowseControlsResponse;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.CountControlsRequest;
import com.acme.compliance.v1.CountControlsResponse;
import com.acme.compliance.v1.ExportControlsRequest;
import com.acme.compliance.v1.ExportControlsResponse;
import com.acme.compliance.v1.FindControlsRequest;
import com.acme.compliance.v1.FindControlsResponse;
import com.acme.compliance.v1.LookupControlRequest;
import com.acme.compliance.v1.LookupControlResponse;
import com.acme.compliance.v1.PeekControlRequest;
import com.acme.compliance.v1.PeekControlResponse;
import com.acme.compliance.v1.ResolveControlsRequest;
import com.acme.compliance.v1.ResolveControlsResponse;
import com.acme.compliance.v1.SuggestControlsRequest;
import com.acme.compliance.v1.SuggestControlsResponse;
import com.acme.compliance.v1.SummarizeControlsRequest;
import com.acme.compliance.v1.SummarizeControlsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about controls, for screens and reports.
 */
@GrpcService
public class ControlQueryEndpoint extends ControlQueryServiceGrpc.ControlQueryServiceImplBase {
    protected final ControlQueryApplication application;

    public ControlQueryEndpoint(ControlQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a control.
     */
    @Override
    public void lookupControl(LookupControlRequest request, StreamObserver<LookupControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupControl(request));
    }

    /**
     * Lists controls a page at a time.
     */
    @Override
    public void browseControls(BrowseControlsRequest request, StreamObserver<BrowseControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseControls(request));
    }

    /**
     * Lists controls a page at a time.
     */
    @Override
    public void findControls(FindControlsRequest request, StreamObserver<FindControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findControls(request));
    }

    /**
     * Counts controls by status.
     */
    @Override
    public void countControls(CountControlsRequest request, StreamObserver<CountControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countControls(request));
    }

    /**
     * Returns several controls at once.
     */
    @Override
    public void resolveControls(ResolveControlsRequest request, StreamObserver<ResolveControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveControls(request));
    }

    /**
     * Streams every matching control.
     */
    @Override
    public void exportControls(ExportControlsRequest request, StreamObserver<ExportControlsResponse> observer) {
        try {
            var snapshot = application.exportControls(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a control.
     */
    @Override
    public void summarizeControls(SummarizeControlsRequest request, StreamObserver<SummarizeControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeControls(request));
    }

    /**
     * Lists controls a page at a time.
     */
    @Override
    public void suggestControls(SuggestControlsRequest request, StreamObserver<SuggestControlsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestControls(request));
    }

    /**
     * Returns a control.
     */
    @Override
    public void peekControl(PeekControlRequest request, StreamObserver<PeekControlResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekControl(request));
    }
}

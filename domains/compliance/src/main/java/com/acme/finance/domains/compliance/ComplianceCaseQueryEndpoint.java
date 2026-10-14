package com.acme.finance.domains.compliance;

import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.compliance.v1.CountComplianceCasesRequest;
import com.acme.compliance.v1.CountComplianceCasesResponse;
import com.acme.compliance.v1.ExportComplianceCasesRequest;
import com.acme.compliance.v1.ExportComplianceCasesResponse;
import com.acme.compliance.v1.FindComplianceCasesRequest;
import com.acme.compliance.v1.FindComplianceCasesResponse;
import com.acme.compliance.v1.LookupComplianceCaseRequest;
import com.acme.compliance.v1.LookupComplianceCaseResponse;
import com.acme.compliance.v1.PeekComplianceCaseRequest;
import com.acme.compliance.v1.PeekComplianceCaseResponse;
import com.acme.compliance.v1.ResolveComplianceCasesRequest;
import com.acme.compliance.v1.ResolveComplianceCasesResponse;
import com.acme.compliance.v1.SuggestComplianceCasesRequest;
import com.acme.compliance.v1.SuggestComplianceCasesResponse;
import com.acme.compliance.v1.SummarizeComplianceCasesRequest;
import com.acme.compliance.v1.SummarizeComplianceCasesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about compliance cases, for screens and reports.
 */
@GrpcService
public class ComplianceCaseQueryEndpoint extends ComplianceCaseQueryServiceGrpc.ComplianceCaseQueryServiceImplBase {
    protected final ComplianceCaseQueryApplication application;

    public ComplianceCaseQueryEndpoint(ComplianceCaseQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a compliance case.
     */
    @Override
    public void lookupComplianceCase(LookupComplianceCaseRequest request, StreamObserver<LookupComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupComplianceCase(request));
    }

    /**
     * Lists compliance cases a page at a time.
     */
    @Override
    public void browseComplianceCases(BrowseComplianceCasesRequest request, StreamObserver<BrowseComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseComplianceCases(request));
    }

    /**
     * Lists compliance cases a page at a time.
     */
    @Override
    public void findComplianceCases(FindComplianceCasesRequest request, StreamObserver<FindComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findComplianceCases(request));
    }

    /**
     * Counts compliance cases by status.
     */
    @Override
    public void countComplianceCases(CountComplianceCasesRequest request, StreamObserver<CountComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countComplianceCases(request));
    }

    /**
     * Returns several compliance cases at once.
     */
    @Override
    public void resolveComplianceCases(ResolveComplianceCasesRequest request, StreamObserver<ResolveComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveComplianceCases(request));
    }

    /**
     * Streams every matching compliance case.
     */
    @Override
    public void exportComplianceCases(ExportComplianceCasesRequest request, StreamObserver<ExportComplianceCasesResponse> observer) {
        try {
            var snapshot = application.exportComplianceCases(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes a compliance case.
     */
    @Override
    public void summarizeComplianceCases(SummarizeComplianceCasesRequest request, StreamObserver<SummarizeComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeComplianceCases(request));
    }

    /**
     * Lists compliance cases a page at a time.
     */
    @Override
    public void suggestComplianceCases(SuggestComplianceCasesRequest request, StreamObserver<SuggestComplianceCasesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestComplianceCases(request));
    }

    /**
     * Returns a compliance case.
     */
    @Override
    public void peekComplianceCase(PeekComplianceCaseRequest request, StreamObserver<PeekComplianceCaseResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekComplianceCase(request));
    }
}

package com.acme.finance.domains.invoicing;

import com.acme.finance.platform.RpcSupport;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesRequest;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesResponse;
import com.acme.invoicing.v1.CountInvoiceTemplatesRequest;
import com.acme.invoicing.v1.CountInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ExportInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ExportInvoiceTemplatesResponse;
import com.acme.invoicing.v1.FindInvoiceTemplatesRequest;
import com.acme.invoicing.v1.FindInvoiceTemplatesResponse;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.LookupInvoiceTemplateRequest;
import com.acme.invoicing.v1.LookupInvoiceTemplateResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.SuggestInvoiceTemplatesRequest;
import com.acme.invoicing.v1.SuggestInvoiceTemplatesResponse;
import com.acme.invoicing.v1.SummarizeInvoiceTemplatesRequest;
import com.acme.invoicing.v1.SummarizeInvoiceTemplatesResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about invoice templates, for screens and reports.
 */
@GrpcService
public class InvoiceTemplateQueryEndpoint extends InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceImplBase {
    protected final InvoiceTemplateQueryApplication application;

    public InvoiceTemplateQueryEndpoint(InvoiceTemplateQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns an invoice template.
     */
    @Override
    public void lookupInvoiceTemplate(LookupInvoiceTemplateRequest request, StreamObserver<LookupInvoiceTemplateResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupInvoiceTemplate(request));
    }

    /**
     * Lists invoice templates a page at a time.
     */
    @Override
    public void browseInvoiceTemplates(BrowseInvoiceTemplatesRequest request, StreamObserver<BrowseInvoiceTemplatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseInvoiceTemplates(request));
    }

    /**
     * Lists invoice templates a page at a time.
     */
    @Override
    public void findInvoiceTemplates(FindInvoiceTemplatesRequest request, StreamObserver<FindInvoiceTemplatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findInvoiceTemplates(request));
    }

    /**
     * Counts invoice templates by status.
     */
    @Override
    public void countInvoiceTemplates(CountInvoiceTemplatesRequest request, StreamObserver<CountInvoiceTemplatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countInvoiceTemplates(request));
    }

    /**
     * Returns several invoice templates at once.
     */
    @Override
    public void resolveInvoiceTemplates(ResolveInvoiceTemplatesRequest request, StreamObserver<ResolveInvoiceTemplatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveInvoiceTemplates(request));
    }

    /**
     * Streams every matching invoice template.
     */
    @Override
    public void exportInvoiceTemplates(ExportInvoiceTemplatesRequest request, StreamObserver<ExportInvoiceTemplatesResponse> observer) {
        try {
            var snapshot = application.exportInvoiceTemplates(request);
            if (!io.grpc.Context.current().isCancelled()) {
                observer.onNext(snapshot);
                observer.onCompleted();
            }
        } catch (RuntimeException failure) {
            observer.onError(RpcSupport.status(failure));
        }
    }

    /**
     * Summarizes an invoice template.
     */
    @Override
    public void summarizeInvoiceTemplates(SummarizeInvoiceTemplatesRequest request, StreamObserver<SummarizeInvoiceTemplatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeInvoiceTemplates(request));
    }

    /**
     * Lists invoice templates a page at a time.
     */
    @Override
    public void suggestInvoiceTemplates(SuggestInvoiceTemplatesRequest request, StreamObserver<SuggestInvoiceTemplatesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestInvoiceTemplates(request));
    }
}

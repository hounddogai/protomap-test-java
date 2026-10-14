package com.acme.finance.domains.billing;

import com.acme.billing.v1.BrowseInvoicesRequest;
import com.acme.billing.v1.BrowseInvoicesResponse;
import com.acme.billing.v1.CountInvoicesRequest;
import com.acme.billing.v1.CountInvoicesResponse;
import com.acme.billing.v1.FindInvoicesRequest;
import com.acme.billing.v1.FindInvoicesResponse;
import com.acme.billing.v1.InvoiceQueryServiceGrpc;
import com.acme.billing.v1.LookupInvoiceRequest;
import com.acme.billing.v1.LookupInvoiceResponse;
import com.acme.billing.v1.PeekInvoiceRequest;
import com.acme.billing.v1.PeekInvoiceResponse;
import com.acme.billing.v1.ResolveInvoicesRequest;
import com.acme.billing.v1.ResolveInvoicesResponse;
import com.acme.billing.v1.SuggestInvoicesRequest;
import com.acme.billing.v1.SuggestInvoicesResponse;
import com.acme.billing.v1.SummarizeInvoicesRequest;
import com.acme.billing.v1.SummarizeInvoicesResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about invoices, for screens and reports.
 */
@GrpcService
public class InvoiceQueryEndpoint extends InvoiceQueryServiceGrpc.InvoiceQueryServiceImplBase {
    protected final InvoiceQueryApplication application;

    public InvoiceQueryEndpoint(InvoiceQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns an invoice.
     */
    @Override
    public void lookupInvoice(LookupInvoiceRequest request, StreamObserver<LookupInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupInvoice(request));
    }

    /**
     * Lists invoices a page at a time.
     */
    @Override
    public void browseInvoices(BrowseInvoicesRequest request, StreamObserver<BrowseInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseInvoices(request));
    }

    /**
     * Lists invoices a page at a time.
     */
    @Override
    public void findInvoices(FindInvoicesRequest request, StreamObserver<FindInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.findInvoices(request));
    }

    /**
     * Counts invoices by status.
     */
    @Override
    public void countInvoices(CountInvoicesRequest request, StreamObserver<CountInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.countInvoices(request));
    }

    /**
     * Returns several invoices at once.
     */
    @Override
    public void resolveInvoices(ResolveInvoicesRequest request, StreamObserver<ResolveInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveInvoices(request));
    }

    /**
     * Summarizes an invoice.
     */
    @Override
    public void summarizeInvoices(SummarizeInvoicesRequest request, StreamObserver<SummarizeInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeInvoices(request));
    }

    /**
     * Lists invoices a page at a time.
     */
    @Override
    public void suggestInvoices(SuggestInvoicesRequest request, StreamObserver<SuggestInvoicesResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestInvoices(request));
    }

    /**
     * Returns an invoice.
     */
    @Override
    public void peekInvoice(PeekInvoiceRequest request, StreamObserver<PeekInvoiceResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekInvoice(request));
    }
}

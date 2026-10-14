package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.BrowseAuditEventsRequest;
import com.acme.audit.v1.BrowseAuditEventsResponse;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.LookupAuditEventRequest;
import com.acme.audit.v1.LookupAuditEventResponse;
import com.acme.audit.v1.ResolveAuditEventsRequest;
import com.acme.audit.v1.ResolveAuditEventsResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.finance.platform.RpcSupport;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about audit events, for screens and reports.
 */
@GrpcService
public class AuditEventQueryEndpoint extends AuditEventQueryServiceGrpc.AuditEventQueryServiceImplBase {
    protected final AuditEventQueryApplication application;

    public AuditEventQueryEndpoint(AuditEventQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns an audit event.
     */
    @Override
    public void lookupAuditEvent(LookupAuditEventRequest request, StreamObserver<LookupAuditEventResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupAuditEvent(request));
    }

    /**
     * Lists audit events a page at a time.
     */
    @Override
    public void browseAuditEvents(BrowseAuditEventsRequest request, StreamObserver<BrowseAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseAuditEvents(request));
    }

    /**
     * Lists audit events a page at a time.
     */
    @Override
    public void findAuditEvents(FindAuditEventsRequest request, StreamObserver<FindAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.findAuditEvents(request));
    }

    /**
     * Counts audit events by status.
     */
    @Override
    public void countAuditEvents(CountAuditEventsRequest request, StreamObserver<CountAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.countAuditEvents(request));
    }

    /**
     * Returns several audit events at once.
     */
    @Override
    public void resolveAuditEvents(ResolveAuditEventsRequest request, StreamObserver<ResolveAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveAuditEvents(request));
    }

    /**
     * Lists audit events a page at a time.
     */
    @Override
    public void suggestAuditEvents(SuggestAuditEventsRequest request, StreamObserver<SuggestAuditEventsResponse> observer) {
        RpcSupport.unary(observer, () -> application.suggestAuditEvents(request));
    }
}

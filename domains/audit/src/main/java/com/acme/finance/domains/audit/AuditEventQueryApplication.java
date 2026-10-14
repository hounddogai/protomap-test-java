package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.BrowseAuditEventsRequest;
import com.acme.audit.v1.BrowseAuditEventsResponse;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.LookupAuditEventRequest;
import com.acme.audit.v1.LookupAuditEventResponse;
import com.acme.audit.v1.PushAuditTrailRequest;
import com.acme.audit.v1.PushAuditTrailResponse;
import com.acme.audit.v1.ResolveAuditEventsRequest;
import com.acme.audit.v1.ResolveAuditEventsResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about audit events, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class AuditEventQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditEventQueryApplication.class);

    private final ChannelRegistry channels;

    private final AuditRepository repository;

    public AuditEventQueryApplication(
            ChannelRegistry channels,
            AuditRepository repository) {
        this.channels = channels;
        this.repository = repository;
    }

    /**
     * Returns an audit event.
     */
    public LookupAuditEventResponse lookupAuditEvent(LookupAuditEventRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupAuditEvent")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getAuditEventId(), "LookupAuditEvent");
        return LookupAuditEventResponse.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setAuditEventId(request.getAuditEventId())
                        .build())
                .build();
    }

    /**
     * Lists audit events a page at a time.
     */
    public BrowseAuditEventsResponse browseAuditEvents(BrowseAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "BrowseAuditEvents");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseAuditEvents", request.getPage().getPageToken());
        return BrowseAuditEventsResponse.getDefaultInstance();
    }

    /**
     * Lists audit events a page at a time.
     */
    public FindAuditEventsResponse findAuditEvents(FindAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "FindAuditEvents");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindAuditEvents", request.getPage().getPageToken());
        return FindAuditEventsResponse.getDefaultInstance();
    }

    /**
     * Counts audit events by status.
     */
    public CountAuditEventsResponse countAuditEvents(CountAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "CountAuditEvents");

        var outbound0 = PushAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setIssuedAt(request.getFilter().getCreatedAfter())
                        .addLines(AuditTrailLine.newBuilder()
                                .putAllAttributes(request.getFilter().getLabelsMap())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PushAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.AuditTrailSyncService");
        var selected0 = AuditTrailSyncServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.pushAuditTrail(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountAuditEvents", request.getPage().getPageToken());
        return CountAuditEventsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns several audit events at once.
     */
    public ResolveAuditEventsResponse resolveAuditEvents(ResolveAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "ResolveAuditEvents");
        return ResolveAuditEventsResponse.getDefaultInstance();
    }

    /**
     * Lists audit events a page at a time.
     */
    public SuggestAuditEventsResponse suggestAuditEvents(SuggestAuditEventsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestAuditEvents")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditEvent", request.getContext().getTraceId(), "SuggestAuditEvents");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestAuditEvents", request.getPage().getPageToken());
        return SuggestAuditEventsResponse.getDefaultInstance();
    }
}

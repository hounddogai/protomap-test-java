package com.acme.finance.domains.audit;

import com.acme.audit.v1.CancelRetentionPolicyExportRequest;
import com.acme.audit.v1.CancelRetentionPolicyExportResponse;
import com.acme.audit.v1.GetRetentionPolicyExportRequest;
import com.acme.audit.v1.GetRetentionPolicyExportResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetryRetentionPolicyExportRequest;
import com.acme.audit.v1.RetryRetentionPolicyExportResponse;
import com.acme.audit.v1.ScheduleRetentionPolicyExportRequest;
import com.acme.audit.v1.ScheduleRetentionPolicyExportResponse;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.audit.v1.SyncRetentionPoliciesRequest;
import com.acme.audit.v1.SyncRetentionPoliciesResponse;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Exports retention policies to files and other systems. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class RetentionPolicyExportApplication {
    private static final Logger log = LoggerFactory.getLogger(RetentionPolicyExportApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final AuditRepository repository;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public RetentionPolicyExportApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            AuditRepository repository,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.repository = repository;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    /**
     * Streams every matching retention policy.
     */
    public StreamRetentionPolicyExportResponse streamRetentionPolicyExport(StreamRetentionPolicyExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamRetentionPolicyExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getContext().getTraceId(), "StreamRetentionPolicyExport");
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamRetentionPolicyExport", request.getFilter().getQuery());
        return StreamRetentionPolicyExportResponse.getDefaultInstance();
    }

    /**
     * Returns a retention policy.
     */
    public GetRetentionPolicyExportResponse getRetentionPolicyExport(GetRetentionPolicyExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetRetentionPolicyExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "GetRetentionPolicyExport");

        var outbound0 = SyncRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncRetentionPoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncRetentionPolicies(value)))
                .collect(Collectors.toList()).getFirst();
        return GetRetentionPolicyExportResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setDescription(reply0.getRetentionPolicy().getDescription())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Cancels a retention policy.
     */
    public CancelRetentionPolicyExportResponse cancelRetentionPolicyExport(CancelRetentionPolicyExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelRetentionPolicyExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "CancelRetentionPolicyExport");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelRetentionPolicyExport", request.getDetails().getReasonCode());
        return CancelRetentionPolicyExportResponse.newBuilder()
                .setOutcome(CancelRetentionPolicyExportResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Schedules a retention policy.
     */
    public ScheduleRetentionPolicyExportResponse scheduleRetentionPolicyExport(ScheduleRetentionPolicyExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleRetentionPolicyExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "ScheduleRetentionPolicyExport");

        var outbound0 = SuggestVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .setOwnerId(request.getRetentionPolicyId())
                        .build())
                .build();
        var reply0 = RpcSupport.retryRead(() -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestVenues(outbound0));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleRetentionPolicyExport", request.getDetails().getFormat());
        return ScheduleRetentionPolicyExportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Retrys a retention policy.
     */
    public RetryRetentionPolicyExportResponse retryRetentionPolicyExport(RetryRetentionPolicyExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryRetentionPolicyExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryRetentionPolicyExport", request.getRetentionPolicy().getRetentionPolicyId());
        return RetryRetentionPolicyExportResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .putAllParameters(request.getDetails().getParametersMap())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Shares a retention policy.
     */
    public ShareRetentionPolicyExportResponse shareRetentionPolicyExport(ShareRetentionPolicyExportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareRetentionPolicyExport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "ShareRetentionPolicyExport");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareRetentionPolicyExport", request.getDetails().getRecipient().getPhone());
        return ShareRetentionPolicyExportResponse.newBuilder()
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }
}

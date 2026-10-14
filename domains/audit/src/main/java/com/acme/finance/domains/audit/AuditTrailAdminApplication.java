package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveAuditTrailRequest;
import com.acme.audit.v1.ArchiveAuditTrailResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.InspectAuditTrailRequest;
import com.acme.audit.v1.InspectAuditTrailResponse;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.LockAuditTrailRequest;
import com.acme.audit.v1.LockAuditTrailResponse;
import com.acme.audit.v1.PurgeAuditTrailRequest;
import com.acme.audit.v1.PurgeAuditTrailResponse;
import com.acme.audit.v1.ReassignAuditTrailRequest;
import com.acme.audit.v1.ReassignAuditTrailResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.audit.v1.RestoreAuditTrailRequest;
import com.acme.audit.v1.RestoreAuditTrailResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.TransferAuditTrailRequest;
import com.acme.audit.v1.TransferAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Operator actions on audit trails that support staff run by hand. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class AuditTrailAdminApplication {
    private static final Logger log = LoggerFactory.getLogger(AuditTrailAdminApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final AuditRepository repository;

    public AuditTrailAdminApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            AuditRepository repository) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
    }

    /**
     * Archives an audit trail.
     */
    public ArchiveAuditTrailResponse archiveAuditTrail(ArchiveAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ArchiveAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "ArchiveAuditTrail");

        var outbound0 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input0 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ArchiveAuditTrail", request.getDetails().getReasonCode());
        return ArchiveAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .setOutcome(ArchiveAuditTrailResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Restores an audit trail.
     */
    public RestoreAuditTrailResponse restoreAuditTrail(RestoreAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RestoreAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "RestoreAuditTrail");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RestoreAuditTrail", request.getDetails().getReasonCode());
        return RestoreAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .setOutcome(RestoreAuditTrailResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Deletes an audit trail.
     */
    public PurgeAuditTrailResponse purgeAuditTrail(PurgeAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PurgeAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "PurgeAuditTrail");
        log.atDebug().addKeyValue("auditValue", request.getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PurgeAuditTrail", request.getAuditTrailId());
        return PurgeAuditTrailResponse.getDefaultInstance();
    }

    /**
     * Reassigns an audit trail.
     */
    public ReassignAuditTrailResponse reassignAuditTrail(ReassignAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReassignAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "ReassignAuditTrail");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getAssignee().getUserId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReassignAuditTrail", request.getDetails().getAssignee().getUserId());
        return ReassignAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(request.getDetails().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Locks an audit trail.
     */
    public LockAuditTrailResponse lockAuditTrail(LockAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LockAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "LockAuditTrail");

        var outbound0 = (ListFiscalPeriodSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFiscalPeriodSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .build())
                .build()).toBuilder().build();
        var reply0 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFiscalPeriodSubscriptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LockAuditTrail", request.getDetails().getReasonCode());
        return LockAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .setOutcome(LockAuditTrailResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Transfers an audit trail.
     */
    public TransferAuditTrailResponse transferAuditTrail(TransferAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TransferAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "TransferAuditTrail");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TransferAuditTrail", request.getDetails().getReasonCode());
        return TransferAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .setOutcome(TransferAuditTrailResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Repairs an audit trail.
     */
    public RepairAuditTrailResponse repairAuditTrail(RepairAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RepairAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAuditTrail(request.getContext().getTenantId(), request.getAuditTrail());

        var outbound0 = UpdateMetricSeriesRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getAuditTrail().getBillingAddress().getLinesList())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getAuditTrail().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateMetricSeries(value));
        log.atDebug().addKeyValue("auditValue", request.getAuditTrail().getAuditTrailId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RepairAuditTrail", request.getAuditTrail().getAuditTrailId());
        return RepairAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .setCounterparty(RetentionPolicyRef.newBuilder()
                                .setDisplayName(reply0.getMetricSeries().getOwner().getDisplayName())
                                .build())
                        .setDueAt(request.getAuditTrail().getDueAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an audit trail.
     */
    public InspectAuditTrailResponse inspectAuditTrail(InspectAuditTrailRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectAuditTrail")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AuditTrail", request.getAuditTrailId(), "InspectAuditTrail");

        var outbound0 = ListAuditTrailExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailExportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getAuditTrailId())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.audit.v1.AuditTrailExportService");
        var selected0 = AuditTrailExportServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.listAuditTrailExports(outbound0);
        return InspectAuditTrailResponse.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAuditTrailId(request.getAuditTrailId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}

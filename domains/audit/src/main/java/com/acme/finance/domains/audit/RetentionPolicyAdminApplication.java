package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveRetentionPolicyRequest;
import com.acme.audit.v1.ArchiveRetentionPolicyResponse;
import com.acme.audit.v1.GetRetentionPolicyExportRequest;
import com.acme.audit.v1.GetRetentionPolicyExportResponse;
import com.acme.audit.v1.PurgeRetentionPolicyRequest;
import com.acme.audit.v1.PurgeRetentionPolicyResponse;
import com.acme.audit.v1.RepairRetentionPolicyRequest;
import com.acme.audit.v1.RepairRetentionPolicyResponse;
import com.acme.audit.v1.RestoreRetentionPolicyRequest;
import com.acme.audit.v1.RestoreRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.TransferRetentionPolicyRequest;
import com.acme.audit.v1.TransferRetentionPolicyResponse;
import com.acme.audit.v1.UnlockRetentionPolicyRequest;
import com.acme.audit.v1.UnlockRetentionPolicyResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.LintMetricSeriesesRequest;
import com.acme.metrics.v1.LintMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesComponent;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.notifications.v1.BatchGetNotificationsRequest;
import com.acme.notifications.v1.BatchGetNotificationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Operator actions on retention policies that support staff run by hand. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class RetentionPolicyAdminApplication {
    private static final Logger log = LoggerFactory.getLogger(RetentionPolicyAdminApplication.class);

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking;

    private final AuditRepository repository;

    public RetentionPolicyAdminApplication(
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            NotificationServiceGrpc.NotificationServiceBlockingStub notificationsNotificationServiceBlocking,
            AuditRepository repository) {
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.notificationsNotificationServiceBlocking = notificationsNotificationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Archives a retention policy.
     */
    public ArchiveRetentionPolicyResponse archiveRetentionPolicy(ArchiveRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ArchiveRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "ArchiveRetentionPolicy");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ArchiveRetentionPolicy", request.getDetails().getReasonCode());
        return ArchiveRetentionPolicyResponse.newBuilder()
                .setOutcome(ArchiveRetentionPolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Restores a retention policy.
     */
    public RestoreRetentionPolicyResponse restoreRetentionPolicy(RestoreRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RestoreRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "RestoreRetentionPolicy");
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RestoreRetentionPolicy", request.getDetails().getReasonCode());
        return RestoreRetentionPolicyResponse.newBuilder()
                .setOutcome(RestoreRetentionPolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Deletes a retention policy.
     */
    public PurgeRetentionPolicyResponse purgeRetentionPolicy(PurgeRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PurgeRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "PurgeRetentionPolicy");
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PurgeRetentionPolicy", request.getRetentionPolicyId());
        return PurgeRetentionPolicyResponse.getDefaultInstance();
    }

    /**
     * Unlocks a retention policy.
     */
    public UnlockRetentionPolicyResponse unlockRetentionPolicy(UnlockRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "UnlockRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "UnlockRetentionPolicy");

        var outbound0 = (LintMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintMetricSeriesesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintMetricSerieses(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "UnlockRetentionPolicy", request.getDetails().getReasonCode());
        return UnlockRetentionPolicyResponse.newBuilder()
                .setOutcome(UnlockRetentionPolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setDescription(reply0.getMetricSeries().getComponentsOrDefault("primary", MetricSeriesComponent.getDefaultInstance()).getDescription())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getMetricSeries().getOwner().getDisplayName())
                                .build())
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .build())
                .build();
    }

    /**
     * Transfers a retention policy.
     */
    public TransferRetentionPolicyResponse transferRetentionPolicy(TransferRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TransferRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "RetentionPolicy", request.getRetentionPolicyId(), "TransferRetentionPolicy");

        var outbound0 = GetRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setRetentionPolicyId(request.getRetentionPolicyId())
                .build();
        RpcPort<GetRetentionPolicyExportRequest, GetRetentionPolicyExportResponse> port0 = RpcPort.adapt(auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::getRetentionPolicyExport);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TransferRetentionPolicy", request.getDetails().getReasonCode());
        return TransferRetentionPolicyResponse.newBuilder()
                .setOutcome(TransferRetentionPolicyResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .setStatus(reply0.getRetentionPolicy().getStatus())
                        .build())
                .build();
    }

    /**
     * Repairs a retention policy.
     */
    public RepairRetentionPolicyResponse repairRetentionPolicy(RepairRetentionPolicyRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RepairRetentionPolicy")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveRetentionPolicy(request.getContext().getTenantId(), request.getRetentionPolicy());

        var outbound0 = BatchGetNotificationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply0 = notificationsNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetNotifications(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getRetentionPolicy().getRetentionPolicyId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RepairRetentionPolicy", request.getRetentionPolicy().getRetentionPolicyId());
        return RepairRetentionPolicyResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setRetentionPolicyId(request.getRetentionPolicyId())
                        .setStatus(request.getRetentionPolicy().getStatus())
                        .build())
                .build();
    }
}

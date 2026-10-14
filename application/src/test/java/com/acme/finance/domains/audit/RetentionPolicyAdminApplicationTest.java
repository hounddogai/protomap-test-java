package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveRetentionPolicyRequest;
import com.acme.audit.v1.ArchiveRetentionPolicyResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
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
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.LintMetricSeriesesRequest;
import com.acme.metrics.v1.LintMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesComponent;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.notifications.v1.BatchGetNotificationsRequest;
import com.acme.notifications.v1.BatchGetNotificationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class RetentionPolicyAdminApplicationTest {
    @Test
    void unlockRetentionPolicyPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new RetentionPolicyAdminApplication(
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(UnlockRetentionPolicyRequest.getDefaultInstance());
        var response = application.unlockRetentionPolicy(request);
        var sent0 = channel.request("acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses"), "metric_series", "components", "description"),
                ProbeSupport.value(response, "retention_policy", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses"), "metric_series", "owner", "display_name"),
                ProbeSupport.value(response, "retention_policy", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "retention_policy_id"),
                ProbeSupport.value(response, "retention_policy", "retention_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}

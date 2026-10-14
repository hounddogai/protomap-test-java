package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveAuditTrailRequest;
import com.acme.audit.v1.ArchiveAuditTrailResponse;
import com.acme.audit.v1.AuditEvent;
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
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AuditTrailAdminApplicationTest {
    @Test
    void repairAuditTrailPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditTrailAdminApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(RepairAuditTrailRequest.getDefaultInstance());
        var response = application.repairAuditTrail(request);
        var sent0 = channel.request("acme.metrics.v1.MetricSeriesService/UpdateMetricSeries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail", "audit", "created_by"),
                ProbeSupport.value(sent0, "metric_series", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail", "billing_address", "lines"),
                ProbeSupport.value(sent0, "metric_series", "address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesService/UpdateMetricSeries"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesService/UpdateMetricSeries"), "metric_series", "owner", "display_name"),
                ProbeSupport.value(response, "audit_trail", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail_id"),
                ProbeSupport.value(response, "audit_trail", "audit_trail_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail", "due_at"),
                ProbeSupport.value(response, "audit_trail", "due_at"));
    }
}

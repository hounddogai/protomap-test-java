package com.acme.finance.domains.audit;

import com.acme.audit.v1.ArchiveAuditEventRequest;
import com.acme.audit.v1.ArchiveAuditEventResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.InspectAuditEventRequest;
import com.acme.audit.v1.InspectAuditEventResponse;
import com.acme.audit.v1.LockAuditEventRequest;
import com.acme.audit.v1.LockAuditEventResponse;
import com.acme.audit.v1.PurgeAuditEventRequest;
import com.acme.audit.v1.PurgeAuditEventResponse;
import com.acme.audit.v1.ReassignAuditEventRequest;
import com.acme.audit.v1.ReassignAuditEventResponse;
import com.acme.audit.v1.RestoreAuditEventRequest;
import com.acme.audit.v1.RestoreAuditEventResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.audit.v1.TransferAuditEventRequest;
import com.acme.audit.v1.TransferAuditEventResponse;
import com.acme.audit.v1.UnlockAuditEventRequest;
import com.acme.audit.v1.UnlockAuditEventResponse;
import com.acme.common.v1.ContactInfo;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AuditEventAdminApplicationTest {
    @Test
    void reassignAuditEventPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditEventAdminApplication(
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ReassignAuditEventRequest.getDefaultInstance());
        var response = application.reassignAuditEvent(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailExportService/RetryAuditTrailExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "assignee", "display_name"),
                ProbeSupport.value(sent0, "audit_trail", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "assignee", "email"),
                ProbeSupport.value(sent0, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailExportService/RetryAuditTrailExport"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event_id"),
                ProbeSupport.value(response, "audit_event", "audit_event_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "assignee", "display_name"),
                ProbeSupport.value(response, "audit_event", "subject", "display_name"));
    }
}

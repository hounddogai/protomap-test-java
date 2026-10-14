package com.acme.finance.domains.audit;

import com.acme.audit.v1.AnnotateAuditEventVersionRequest;
import com.acme.audit.v1.AnnotateAuditEventVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.CompareAuditEventVersionsRequest;
import com.acme.audit.v1.CompareAuditEventVersionsResponse;
import com.acme.audit.v1.GetAuditEventVersionRequest;
import com.acme.audit.v1.GetAuditEventVersionResponse;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.audit.v1.PruneAuditEventVersionsRequest;
import com.acme.audit.v1.PruneAuditEventVersionsResponse;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RevertAuditEventRequest;
import com.acme.audit.v1.RevertAuditEventResponse;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AuditEventHistoryApplicationTest {
    @Test
    void revertAuditEventPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditEventHistoryApplication(
                repository);
        var request = ProbeSupport.sample(RevertAuditEventRequest.getDefaultInstance());
        var response = application.revertAuditEvent(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event_id"),
                ProbeSupport.value(response, "audit_event", "audit_event_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_event", "sequence"),
                ProbeSupport.value(response, "audit_event", "sequence"));
    }
}

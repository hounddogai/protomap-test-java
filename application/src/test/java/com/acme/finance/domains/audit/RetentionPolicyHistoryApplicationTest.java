package com.acme.finance.domains.audit;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.GetRetentionPolicyVersionRequest;
import com.acme.audit.v1.GetRetentionPolicyVersionResponse;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.PruneRetentionPolicyVersionsRequest;
import com.acme.audit.v1.PruneRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class RetentionPolicyHistoryApplicationTest {
    @Test
    void revertRetentionPolicyPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new RetentionPolicyHistoryApplication(
                repository);
        var request = ProbeSupport.sample(RevertRetentionPolicyRequest.getDefaultInstance());
        var response = application.revertRetentionPolicy(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "retention_policy_id"),
                ProbeSupport.value(response, "retention_policy", "retention_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "version"),
                ProbeSupport.value(response, "retention_policy", "version"));
    }
}

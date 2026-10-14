package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.ExplainRetentionPolicyDecisionRequest;
import com.acme.audit.v1.ExplainRetentionPolicyDecisionResponse;
import com.acme.audit.v1.GetRetentionPolicyRulesRequest;
import com.acme.audit.v1.GetRetentionPolicyRulesResponse;
import com.acme.audit.v1.LintRetentionPoliciesRequest;
import com.acme.audit.v1.LintRetentionPoliciesResponse;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.PreviewRetentionPolicyRequest;
import com.acme.audit.v1.PreviewRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.ValidateRetentionPolicyRequest;
import com.acme.audit.v1.ValidateRetentionPolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class RetentionPolicyValidationApplicationTest {
    @Test
    void validateRetentionPolicyPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new RetentionPolicyValidationApplication(
                repository);
        var request = ProbeSupport.sample(ValidateRetentionPolicyRequest.getDefaultInstance());
        var response = application.validateRetentionPolicy(request);
        ProbeSupport.assertValue(ProbeSupport.value(request, "retention_policy_id"),
                ProbeSupport.value(response, "retention_policy", "retention_policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "retention_policy", "name"),
                ProbeSupport.value(response, "retention_policy", "name"));
    }
}

package com.acme.finance.domains.audit;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.CheckAuditTrailEligibilityRequest;
import com.acme.audit.v1.CheckAuditTrailEligibilityResponse;
import com.acme.audit.v1.ExplainAuditTrailDecisionRequest;
import com.acme.audit.v1.ExplainAuditTrailDecisionResponse;
import com.acme.audit.v1.GetAuditTrailRulesRequest;
import com.acme.audit.v1.GetAuditTrailRulesResponse;
import com.acme.audit.v1.LintAuditTrailsRequest;
import com.acme.audit.v1.LintAuditTrailsResponse;
import com.acme.audit.v1.NormalizeAuditTrailRequest;
import com.acme.audit.v1.NormalizeAuditTrailResponse;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.ScoreAuditTrailRequest;
import com.acme.audit.v1.ScoreAuditTrailResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.ValidateAuditTrailRequest;
import com.acme.audit.v1.ValidateAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AuditTrailValidationApplicationTest {
    @Test
    void scoreAuditTrailPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AuditRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAuditEvent(String tenant, AuditEvent value) {}
            public void saveAuditTrail(String tenant, AuditTrail value) {}
            public void saveRetentionPolicy(String tenant, RetentionPolicy value) {}
        };
        var application = new AuditTrailValidationApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ScoreAuditTrailRequest.getDefaultInstance());
        var response = application.scoreAuditTrail(request);
        var sent0 = channel.request("acme.documents.v1.DocumentService/ListDocuments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentService/ListDocuments"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail_id"),
                ProbeSupport.value(response, "audit_trail", "audit_trail_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "audit_trail", "number"),
                ProbeSupport.value(response, "audit_trail", "number"));
    }
}

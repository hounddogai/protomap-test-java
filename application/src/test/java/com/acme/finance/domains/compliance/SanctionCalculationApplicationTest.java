package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.AuditTrailQueryServiceGrpc;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.audit.v1.SummarizeAuditTrailsRequest;
import com.acme.audit.v1.SummarizeAuditTrailsResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateLine;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.CompareSanctionOptionsRequest;
import com.acme.compliance.v1.CompareSanctionOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.EstimateSanctionRequest;
import com.acme.compliance.v1.EstimateSanctionResponse;
import com.acme.compliance.v1.ExplainSanctionCalculationRequest;
import com.acme.compliance.v1.ExplainSanctionCalculationResponse;
import com.acme.compliance.v1.GetSanctionRatesRequest;
import com.acme.compliance.v1.GetSanctionRatesResponse;
import com.acme.compliance.v1.NormalizeControlRequest;
import com.acme.compliance.v1.NormalizeControlResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SimulateSanctionRequest;
import com.acme.compliance.v1.SimulateSanctionResponse;
import com.acme.configuration.v1.CompareConfigSetOptionsRequest;
import com.acme.configuration.v1.CompareConfigSetOptionsResponse;
import com.acme.configuration.v1.ConfigSet;
import com.acme.configuration.v1.ConfigSetCalculationServiceGrpc;
import com.acme.configuration.v1.ExplainConfigSetCalculationRequest;
import com.acme.configuration.v1.ExplainConfigSetCalculationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.RecordLedgerAccountVersionRequest;
import com.acme.ledger.v1.RecordLedgerAccountVersionResponse;
import com.acme.messaging.v1.ConversationQueryServiceGrpc;
import com.acme.messaging.v1.ResolveConversationsRequest;
import com.acme.messaging.v1.ResolveConversationsResponse;
import com.acme.risk.v1.ExplainRiskScenarioDecisionRequest;
import com.acme.risk.v1.ExplainRiskScenarioDecisionResponse;
import com.acme.risk.v1.RiskScenario;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class SanctionCalculationApplicationTest {
    @Test
    void simulateSanctionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new SanctionCalculationApplication(
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                AuditTrailQueryServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                ConfigSetCalculationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PolicyCalculationServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                ConversationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(SimulateSanctionRequest.getDefaultInstance());
        var response = application.simulateSanction(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailQueryService/SummarizeAuditTrails");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "attributes"),
                ProbeSupport.value(sent0, "details", "filters"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailExportService/ListAuditTrailExports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "location", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "audit", "updated_by"),
                ProbeSupport.value(sent2, "compliance_case", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent2, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "location", "postal_code"),
                ProbeSupport.value(sent2, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "recipient"),
                ProbeSupport.value(sent2, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailQueryService/SummarizeAuditTrails"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailExportService/ListAuditTrailExports"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailExportService/ListAuditTrailExports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "compliance_case", "assignee", "user_id"),
                ProbeSupport.value(response, "sanction", "children", "children", "manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction_id"),
                ProbeSupport.value(response, "sanction", "sanction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction", "depth"),
                ProbeSupport.value(response, "sanction", "depth"));
    }
}

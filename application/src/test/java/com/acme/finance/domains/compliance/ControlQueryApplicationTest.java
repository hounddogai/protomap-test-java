package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.ExplainAuditEventDecisionRequest;
import com.acme.audit.v1.ExplainAuditEventDecisionResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.BrowseControlsRequest;
import com.acme.compliance.v1.BrowseControlsResponse;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityRequest;
import com.acme.compliance.v1.CheckComplianceCaseEligibilityResponse;
import com.acme.compliance.v1.CheckControlEligibilityRequest;
import com.acme.compliance.v1.CheckControlEligibilityResponse;
import com.acme.compliance.v1.CheckSanctionEligibilityRequest;
import com.acme.compliance.v1.CheckSanctionEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.CountControlsRequest;
import com.acme.compliance.v1.CountControlsResponse;
import com.acme.compliance.v1.EstimateControlRequest;
import com.acme.compliance.v1.EstimateControlResponse;
import com.acme.compliance.v1.EstimateSanctionRequest;
import com.acme.compliance.v1.EstimateSanctionResponse;
import com.acme.compliance.v1.ExportControlsRequest;
import com.acme.compliance.v1.ExportControlsResponse;
import com.acme.compliance.v1.FindControlsRequest;
import com.acme.compliance.v1.FindControlsResponse;
import com.acme.compliance.v1.GetControlRulesRequest;
import com.acme.compliance.v1.GetControlRulesResponse;
import com.acme.compliance.v1.LintAttestationsRequest;
import com.acme.compliance.v1.LintAttestationsResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.LookupControlRequest;
import com.acme.compliance.v1.LookupControlResponse;
import com.acme.compliance.v1.NormalizeAttestationRequest;
import com.acme.compliance.v1.NormalizeAttestationResponse;
import com.acme.compliance.v1.PeekControlRequest;
import com.acme.compliance.v1.PeekControlResponse;
import com.acme.compliance.v1.ResolveControlsRequest;
import com.acme.compliance.v1.ResolveControlsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.SuggestControlsRequest;
import com.acme.compliance.v1.SuggestControlsResponse;
import com.acme.compliance.v1.SummarizeControlsRequest;
import com.acme.compliance.v1.SummarizeControlsResponse;
import com.acme.content.v1.Article;
import com.acme.content.v1.ArticleLine;
import com.acme.content.v1.ArticleLineTax;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.CreateArticleRequest;
import com.acme.content.v1.CreateArticleResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.lending.v1.SummarizeLoanApplicationsRequest;
import com.acme.lending.v1.SummarizeLoanApplicationsResponse;
import com.acme.lineage.v1.BatchGetLineageNodesRequest;
import com.acme.lineage.v1.BatchGetLineageNodesResponse;
import com.acme.lineage.v1.LineageNodeServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ControlQueryApplicationTest {
    @Test
    void exportControlsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ControlQueryApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationValidationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newFutureStub(channel),
                SanctionCalculationServiceGrpc.newBlockingStub(channel),
                SanctionValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newBlockingStub(channel),
                LineageNodeServiceGrpc.newBlockingStub(channel),
                new ControlQueryRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(ExportControlsRequest.getDefaultInstance());
        var response = application.exportControls(request);
        var sent0 = channel.request("acme.lending.v1.LoanApplicationQueryService/SummarizeLoanApplications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "format"),
                ProbeSupport.value(sent0, "details", "format"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "details", "filters"));
        var sent1 = channel.request("acme.compliance.v1.ControlCalculationService/EstimateControl");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent1, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent1, "control", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationQueryService/SummarizeLoanApplications"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationQueryService/SummarizeLoanApplications"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/EstimateControl"), "control", "audit", "updated_at"),
                ProbeSupport.value(response, "control", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/EstimateControl"), "control", "description"),
                ProbeSupport.value(response, "control", "description"));
        var endpoint = new ControlQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportControlsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportControlsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportControls(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeControlsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ControlQueryApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationValidationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newFutureStub(channel),
                SanctionCalculationServiceGrpc.newBlockingStub(channel),
                SanctionValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newBlockingStub(channel),
                LineageNodeServiceGrpc.newBlockingStub(channel),
                new ControlQueryRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(SummarizeControlsRequest.getDefaultInstance());
        var response = application.summarizeControls(request);
        var sent0 = channel.request("acme.compliance.v1.ControlValidationService/GetControlRules");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control_id"),
                ProbeSupport.value(sent0, "control_id"));
        var sent1 = channel.request("acme.content.v1.ArticleService/CreateArticle");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/GetControlRules"), "control", "audit", "created_at"),
                ProbeSupport.value(sent1, "article", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/GetControlRules"), "control", "audit", "created_by"),
                ProbeSupport.value(sent1, "article", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/GetControlRules"), "control", "discount", "amount"),
                ProbeSupport.value(sent1, "article", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/GetControlRules"), "control", "audit", "updated_by"),
                ProbeSupport.value(sent1, "article", "audit", "updated_by"));
        var sent2 = channel.request("acme.compliance.v1.SanctionCalculationService/EstimateSanction");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/GetControlRules"), "control", "owner", "user_id"),
                ProbeSupport.value(sent2, "sanction", "children", "children", "manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.content.v1.ArticleService/CreateArticle"), "article", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "sanction", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.content.v1.ArticleService/CreateArticle"), "article", "audit", "updated_by"),
                ProbeSupport.value(sent2, "sanction", "children", "children", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/GetControlRules"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/GetControlRules"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.content.v1.ArticleService/CreateArticle"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.SanctionCalculationService/EstimateSanction"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}

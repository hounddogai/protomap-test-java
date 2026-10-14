package com.acme.finance.domains.compliance;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlBlockAction;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.CountComplianceCasesRequest;
import com.acme.compliance.v1.CountComplianceCasesResponse;
import com.acme.compliance.v1.ExportComplianceCasesRequest;
import com.acme.compliance.v1.ExportComplianceCasesResponse;
import com.acme.compliance.v1.FindComplianceCasesRequest;
import com.acme.compliance.v1.FindComplianceCasesResponse;
import com.acme.compliance.v1.LookupComplianceCaseRequest;
import com.acme.compliance.v1.LookupComplianceCaseResponse;
import com.acme.compliance.v1.PeekComplianceCaseRequest;
import com.acme.compliance.v1.PeekComplianceCaseResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.ResolveComplianceCasesRequest;
import com.acme.compliance.v1.ResolveComplianceCasesResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SuggestComplianceCasesRequest;
import com.acme.compliance.v1.SuggestComplianceCasesResponse;
import com.acme.compliance.v1.SummarizeComplianceCasesRequest;
import com.acme.compliance.v1.SummarizeComplianceCasesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.risk.v1.CheckRiskScenarioEligibilityRequest;
import com.acme.risk.v1.CheckRiskScenarioEligibilityResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.shows.v1.ResolveShowsRequest;
import com.acme.shows.v1.ResolveShowsResponse;
import com.acme.shows.v1.ShowQueryServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ComplianceCaseQueryApplicationTest {
    @Test
    void findComplianceCasesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ComplianceCaseQueryApplication(
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                SanctionCalculationServiceGrpc.newBlockingStub(channel),
                new ComplianceCaseQueryRequestMapperImpl(),
                repository,
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                ShowQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(FindComplianceCasesRequest.getDefaultInstance());
        var response = application.findComplianceCases(request);
        var sent0 = channel.request("acme.shows.v1.ShowQueryService/ResolveShows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        var sent1 = channel.request("acme.compliance.v1.ControlCalculationService/RecalculateControls");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent1, "control", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.shows.v1.ShowQueryService/ResolveShows"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/RecalculateControls"), "control", "owner", "email"),
                ProbeSupport.value(response, "compliance_cases", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/RecalculateControls"), "control", "audit", "created_by"),
                ProbeSupport.value(response, "compliance_cases", "audit", "created_by"));
    }

    @Test
    void exportComplianceCasesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ComplianceCaseQueryApplication(
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                SanctionCalculationServiceGrpc.newBlockingStub(channel),
                new ComplianceCaseQueryRequestMapperImpl(),
                repository,
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                ShowQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportComplianceCasesRequest.getDefaultInstance());
        var response = application.exportComplianceCases(request);
        var sent0 = channel.request("acme.risk.v1.RiskScenarioValidationService/CheckRiskScenarioEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "risk_scenario", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/CheckRiskScenarioEligibility"), "risk_scenario", "customer", "email"),
                ProbeSupport.value(response, "compliance_case", "requester", "email"));
        var endpoint = new ComplianceCaseQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportComplianceCasesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportComplianceCasesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportComplianceCases(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}

package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.community.v1.ListForumsRequest;
import com.acme.community.v1.ListForumsResponse;
import com.acme.compliance.v1.AdvanceAttestationWorkflowRequest;
import com.acme.compliance.v1.AdvanceAttestationWorkflowResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationLifecycleServiceGrpc;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.AttestationLineTax;
import com.acme.compliance.v1.CancelAttestationWorkflowRequest;
import com.acme.compliance.v1.CancelAttestationWorkflowResponse;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseWorkflowServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlDiscountAction;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.compliance.v1.EstimateSanctionRequest;
import com.acme.compliance.v1.EstimateSanctionResponse;
import com.acme.compliance.v1.ExplainSanctionDecisionRequest;
import com.acme.compliance.v1.ExplainSanctionDecisionResponse;
import com.acme.compliance.v1.InspectAttestationWorkflowRequest;
import com.acme.compliance.v1.InspectAttestationWorkflowResponse;
import com.acme.compliance.v1.ListAttestationWorkflowsRequest;
import com.acme.compliance.v1.ListAttestationWorkflowsResponse;
import com.acme.compliance.v1.PauseAttestationWorkflowRequest;
import com.acme.compliance.v1.PauseAttestationWorkflowResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.RejectAttestationRequest;
import com.acme.compliance.v1.RejectAttestationResponse;
import com.acme.compliance.v1.RejectSanctionRequest;
import com.acme.compliance.v1.RejectSanctionResponse;
import com.acme.compliance.v1.ResumeAttestationWorkflowRequest;
import com.acme.compliance.v1.ResumeAttestationWorkflowResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.RetryAttestationWorkflowRequest;
import com.acme.compliance.v1.RetryAttestationWorkflowResponse;
import com.acme.compliance.v1.RetryControlWorkflowRequest;
import com.acme.compliance.v1.RetryControlWorkflowResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionApprovalServiceGrpc;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.SignalAttestationWorkflowRequest;
import com.acme.compliance.v1.SignalAttestationWorkflowResponse;
import com.acme.compliance.v1.StartAttestationWorkflowRequest;
import com.acme.compliance.v1.StartAttestationWorkflowResponse;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.compliance.v1.SuspendAttestationRequest;
import com.acme.compliance.v1.SuspendAttestationResponse;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanApplicationWorkflowResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.workplaces.v1.Building;
import com.acme.workplaces.v1.BuildingServiceGrpc;
import com.acme.workplaces.v1.CloneBuildingRequest;
import com.acme.workplaces.v1.CloneBuildingResponse;
import com.google.protobuf.Any;
import java.util.List;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class AttestationWorkflowApplicationTest {
    @Test
    void startAttestationWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var listener = new AttestationWorkflowApplicationEvents(AccessPolicyServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AttestationWorkflowApplication.SignalAttestationWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AttestationWorkflowApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ForumServiceGrpc.newBlockingStub(channel),
                AttestationApprovalServiceGrpc.newBlockingStub(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                AttestationLifecycleServiceGrpc.newBlockingStub(channel),
                ComplianceCaseWorkflowServiceGrpc.newBlockingStub(channel),
                ControlWorkflowServiceGrpc.newBlockingStub(channel),
                SanctionApprovalServiceGrpc.newBlockingStub(channel),
                SanctionCalculationServiceGrpc.newBlockingStub(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                SanctionValidationServiceGrpc.newBlockingStub(channel),
                events,
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                new AttestationWorkflowRequestMapperImpl(),
                repository,
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                BuildingServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StartAttestationWorkflowRequest.getDefaultInstance());
        var response = application.startAttestationWorkflow(request);
        var sent0 = channel.request("acme.compliance.v1.ControlWorkflowService/RetryControlWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "lines", "description"),
                ProbeSupport.value(sent0, "control", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "lines", "tax", "amount"),
                ProbeSupport.value(sent0, "control", "discount", "amount"));
        var sent1 = channel.request("acme.compliance.v1.ComplianceCaseWorkflowService/ResumeComplianceCaseWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "audit", "created_by"),
                ProbeSupport.value(sent1, "compliance_case", "audit", "created_by"));
        var sent2 = channel.request("acme.lending.v1.LoanApplicationWorkflowService/AdvanceLoanApplicationWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/RetryControlWorkflow"), "control", "discount", "amount"),
                ProbeSupport.value(sent2, "loan_application", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "loan_application", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "loan_application", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseWorkflowService/ResumeComplianceCaseWorkflow"), "compliance_case", "audit", "updated_by"),
                ProbeSupport.value(sent2, "loan_application", "audit", "updated_by"));
        var sent3 = channel.request("acme.audit.v1.AuditEventNotificationService/StreamAuditEventNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent3, "subscriber", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseWorkflowService/ResumeComplianceCaseWorkflow"), "compliance_case", "requester", "email"),
                ProbeSupport.value(sent3, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseWorkflowService/ResumeComplianceCaseWorkflow"), "compliance_case", "audit", "created_by"),
                ProbeSupport.value(response, "attestation", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationWorkflowService/AdvanceLoanApplicationWorkflow"), "loan_application", "billing_address", "country_code"),
                ProbeSupport.value(response, "attestation", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation_id"),
                ProbeSupport.value(response, "attestation", "attestation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation", "status"),
                ProbeSupport.value(response, "attestation", "status"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AttestationWorkflowApplicationEvents(AccessPolicyServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CloneAccessPolicyResponse>();
        listener.on(new AttestationWorkflowApplication.SignalAttestationWorkflowStage1Requested(ProbeSupport.sample(CloneAccessPolicyRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}

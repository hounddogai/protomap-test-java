package com.acme.finance.domains.compliance;

import com.acme.careplans.v1.CarePlan;
import com.acme.careplans.v1.CarePlanCalculationServiceGrpc;
import com.acme.careplans.v1.CarePlanLine;
import com.acme.careplans.v1.CarePlanLineTax;
import com.acme.careplans.v1.QuoteCarePlanRequest;
import com.acme.careplans.v1.QuoteCarePlanResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffLine;
import com.acme.collections.v1.WriteOffLineTax;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.CalculateComplianceCaseRequest;
import com.acme.compliance.v1.CalculateComplianceCaseResponse;
import com.acme.compliance.v1.CheckControlEligibilityRequest;
import com.acme.compliance.v1.CheckControlEligibilityResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlRouteAction;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetControlRatesRequest;
import com.acme.compliance.v1.GetControlRatesResponse;
import com.acme.compliance.v1.GetControlRulesRequest;
import com.acme.compliance.v1.GetControlRulesResponse;
import com.acme.compliance.v1.LintControlsRequest;
import com.acme.compliance.v1.LintControlsResponse;
import com.acme.compliance.v1.NormalizeControlRequest;
import com.acme.compliance.v1.NormalizeControlResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.RecalculateComplianceCasesRequest;
import com.acme.compliance.v1.RecalculateComplianceCasesResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.ScoreControlRequest;
import com.acme.compliance.v1.ScoreControlResponse;
import com.acme.compliance.v1.SimulateAttestationRequest;
import com.acme.compliance.v1.SimulateAttestationResponse;
import com.acme.compliance.v1.SimulateControlRequest;
import com.acme.compliance.v1.SimulateControlResponse;
import com.acme.compliance.v1.ValidateControlRequest;
import com.acme.compliance.v1.ValidateControlResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.insurance.v1.CheckPremiumEligibilityRequest;
import com.acme.insurance.v1.CheckPremiumEligibilityResponse;
import com.acme.insurance.v1.EstimateUnderwritingRequest;
import com.acme.insurance.v1.EstimateUnderwritingResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingCalculationServiceGrpc;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ControlValidationApplicationTest {
    @Test
    void checkControlEligibilityPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var listener = new ControlValidationApplicationEvents(ControlCalculationServiceGrpc.newStub(channel), VenueQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ControlValidationApplication.ValidateControlStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof ControlValidationApplication.GetControlRulesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ControlValidationApplication(
                CarePlanCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                events,
                UserValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingCalculationServiceGrpc.newBlockingStub(channel),
                new ControlValidationRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(CheckControlEligibilityRequest.getDefaultInstance());
        var response = application.checkControlEligibility(request);
        var sent0 = channel.request("acme.compliance.v1.ControlCalculationService/SimulateControl");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "audit", "updated_by"),
                ProbeSupport.value(sent0, "control", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "owner", "user_id"),
                ProbeSupport.value(sent0, "control", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "owner", "display_name"),
                ProbeSupport.value(sent0, "control", "owner", "display_name"));
        var sent1 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "owner", "email"),
                ProbeSupport.value(sent1, "compliance_case", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/SimulateControl"), "control", "audit", "created_at"),
                ProbeSupport.value(sent1, "compliance_case", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/SimulateControl"), "control", "audit", "updated_at"),
                ProbeSupport.value(sent1, "compliance_case", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/SimulateControl"), "control", "audit", "updated_by"),
                ProbeSupport.value(sent1, "compliance_case", "audit", "updated_by"));
        var sent2 = channel.request("acme.compliance.v1.ControlCalculationService/ExplainControlCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "owner", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "audit", "updated_at"),
                ProbeSupport.value(sent2, "control", "audit", "updated_at"));
        var sent3 = channel.request("acme.compliance.v1.AttestationCalculationService/SimulateAttestation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "compliance_case", "audit", "updated_at"),
                ProbeSupport.value(sent3, "attestation", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/ExplainControlCalculation"), "control", "audit", "etag"),
                ProbeSupport.value(sent3, "attestation", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/SimulateControl"), "control", "owner", "display_name"),
                ProbeSupport.value(response, "control", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/CalculateComplianceCase"), "compliance_case", "priority"),
                ProbeSupport.value(response, "control", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/ExplainControlCalculation"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/ExplainControlCalculation"), "control", "parameters"),
                ProbeSupport.value(response, "control", "parameters"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationCalculationService/SimulateAttestation"), "attestation", "audit", "updated_at"),
                ProbeSupport.value(response, "control", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control_id"),
                ProbeSupport.value(response, "control", "control_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "control", "name"),
                ProbeSupport.value(response, "control", "name"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ControlValidationApplicationEvents(ControlCalculationServiceGrpc.newStub(channel), VenueQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<FindVenuesResponse>();
        listener.on(new ControlValidationApplication.ValidateControlStage0Requested(ProbeSupport.sample(FindVenuesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<GetControlRatesResponse>();
        listener.on(new ControlValidationApplication.GetControlRulesStage0Requested(ProbeSupport.sample(GetControlRatesRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}

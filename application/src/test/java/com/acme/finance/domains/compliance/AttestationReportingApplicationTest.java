package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.NotifyAuditEventWatchersRequest;
import com.acme.audit.v1.NotifyAuditEventWatchersResponse;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.FindAccessPoliciesRequest;
import com.acme.authorization.v1.FindAccessPoliciesResponse;
import com.acme.collections.v1.EscalatePaymentPromiseRequest;
import com.acme.collections.v1.EscalatePaymentPromiseResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.ResumePaymentPromiseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationRef;
import com.acme.compliance.v1.CalculateControlRequest;
import com.acme.compliance.v1.CalculateControlResponse;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.ComplianceCaseValidationServiceGrpc;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlBlockAction;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.compliance.v1.DeleteAttestationReportRequest;
import com.acme.compliance.v1.DeleteAttestationReportResponse;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationRequest;
import com.acme.compliance.v1.ExplainComplianceCaseCalculationResponse;
import com.acme.compliance.v1.FetchAttestationReportRequest;
import com.acme.compliance.v1.FetchAttestationReportResponse;
import com.acme.compliance.v1.GenerateAttestationReportRequest;
import com.acme.compliance.v1.GenerateAttestationReportResponse;
import com.acme.compliance.v1.ListAttestationReportsRequest;
import com.acme.compliance.v1.ListAttestationReportsResponse;
import com.acme.compliance.v1.PreviewComplianceCaseRequest;
import com.acme.compliance.v1.PreviewComplianceCaseResponse;
import com.acme.compliance.v1.RecalculateControlsRequest;
import com.acme.compliance.v1.RecalculateControlsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.ScheduleAttestationReportRequest;
import com.acme.compliance.v1.ScheduleAttestationReportResponse;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.compliance.v1.StreamAttestationMetricsRequest;
import com.acme.compliance.v1.StreamAttestationMetricsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimWorkflowServiceGrpc;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.SignalClaimWorkflowRequest;
import com.acme.insurance.v1.SignalClaimWorkflowResponse;
import com.acme.insurance.v1.Underwriting;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class AttestationReportingApplicationTest {
    @Test
    void streamAttestationMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var listener = new AttestationReportingApplicationEvents(AuditTrailAdminServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AttestationReportingApplication.CountAttestationTotalsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AttestationReportingApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newFutureStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                SanctionCalculationServiceGrpc.newBlockingStub(channel),
                events,
                ClaimWorkflowServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(StreamAttestationMetricsRequest.getDefaultInstance());
        var response = application.streamAttestationMetrics(request);
        var sent0 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/ExplainComplianceCaseCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "compliance_case", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "compliance_case", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/ExplainComplianceCaseCalculation"), "compliance_case", "audit", "created_at"),
                ProbeSupport.value(response, "attestation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/ExplainComplianceCaseCalculation"), "compliance_case", "audit", "updated_at"),
                ProbeSupport.value(response, "attestation", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "attestation", "billing_address", "region"));
        var endpoint = new AttestationReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamAttestationMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamAttestationMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamAttestationMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void shareAttestationReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var listener = new AttestationReportingApplicationEvents(AuditTrailAdminServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AttestationReportingApplication.CountAttestationTotalsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AttestationReportingApplication(
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newFutureStub(channel),
                ComplianceCaseValidationServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                SanctionCalculationServiceGrpc.newBlockingStub(channel),
                events,
                ClaimWorkflowServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ShareAttestationReportRequest.getDefaultInstance());
        var response = application.shareAttestationReport(request);
        var sent0 = channel.request("acme.compliance.v1.SanctionCalculationService/CalculateSanction");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "email"),
                ProbeSupport.value(sent0, "sanction", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "sanction", "children", "manager", "user_id"));
        var sent1 = channel.request("acme.insurance.v1.UnderwritingSyncService/ReconcileUnderwritings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.SanctionCalculationService/CalculateSanction"), "sanction", "audit", "etag"),
                ProbeSupport.value(sent1, "underwriting", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "email"),
                ProbeSupport.value(sent1, "underwriting", "requester", "email"));
        var sent2 = channel.request("acme.insurance.v1.ClaimWorkflowService/SignalClaimWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ReconcileUnderwritings"), "underwriting", "requester", "email"),
                ProbeSupport.value(sent2, "claim", "customer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.SanctionCalculationService/CalculateSanction"), "sanction", "manager", "email"),
                ProbeSupport.value(sent2, "claim", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.SanctionCalculationService/CalculateSanction"), "sanction", "audit", "updated_by"),
                ProbeSupport.value(response, "attestation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.SanctionCalculationService/CalculateSanction"), "sanction", "location", "region"),
                ProbeSupport.value(response, "attestation", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ReconcileUnderwritings"), "underwriting", "sla", "due_at"),
                ProbeSupport.value(response, "attestation", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/ReconcileUnderwritings"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimWorkflowService/SignalClaimWorkflow"), "claim", "audit", "etag"),
                ProbeSupport.value(response, "attestation", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "attestation_id"),
                ProbeSupport.value(response, "attestation", "attestation_id"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AttestationReportingApplicationEvents(AuditTrailAdminServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RepairAuditTrailResponse>();
        listener.on(new AttestationReportingApplication.CountAttestationTotalsStage0Requested(ProbeSupport.sample(RepairAuditTrailRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}

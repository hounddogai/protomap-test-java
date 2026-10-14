package com.acme.finance.domains.compliance;

import com.acme.appointments.v1.Appointment;
import com.acme.appointments.v1.AppointmentParticipant;
import com.acme.appointments.v1.AppointmentServiceGrpc;
import com.acme.appointments.v1.CreateAppointmentRequest;
import com.acme.appointments.v1.CreateAppointmentResponse;
import com.acme.audit.v1.PullRetentionPoliciesRequest;
import com.acme.audit.v1.PullRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ActivateSanctionRequest;
import com.acme.compliance.v1.ActivateSanctionResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.ControlValidationServiceGrpc;
import com.acme.compliance.v1.ExpireSanctionRequest;
import com.acme.compliance.v1.ExpireSanctionResponse;
import com.acme.compliance.v1.ExplainControlCalculationRequest;
import com.acme.compliance.v1.ExplainControlCalculationResponse;
import com.acme.compliance.v1.GetSanctionLifecycleRequest;
import com.acme.compliance.v1.GetSanctionLifecycleResponse;
import com.acme.compliance.v1.PreviewControlRequest;
import com.acme.compliance.v1.PreviewControlResponse;
import com.acme.compliance.v1.ReinstateSanctionRequest;
import com.acme.compliance.v1.ReinstateSanctionResponse;
import com.acme.compliance.v1.RenewSanctionRequest;
import com.acme.compliance.v1.RenewSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.ScheduleSanctionExpiryRequest;
import com.acme.compliance.v1.ScheduleSanctionExpiryResponse;
import com.acme.compliance.v1.SuspendSanctionRequest;
import com.acme.compliance.v1.SuspendSanctionResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.SuggestDocumentsRequest;
import com.acme.documents.v1.SuggestDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.AdvanceLoanWorkflowRequest;
import com.acme.lending.v1.AdvanceLoanWorkflowResponse;
import com.acme.lending.v1.CancelCollateralWorkflowRequest;
import com.acme.lending.v1.CancelCollateralWorkflowResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RejectRiskRegisterRequest;
import com.acme.risk.v1.RejectRiskRegisterResponse;
import com.acme.risk.v1.RejectStressTestRequest;
import com.acme.risk.v1.RejectStressTestResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterApprovalServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.suppliers.v1.BatchCreateSuppliersRequest;
import com.acme.suppliers.v1.BatchCreateSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.travel.v1.SearchTripsRequest;
import com.acme.travel.v1.SearchTripsResponse;
import com.acme.travel.v1.TripServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class SanctionLifecycleApplicationTest {
    @Test
    void reinstateSanctionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new SanctionLifecycleApplication(
                AppointmentServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                ControlValidationServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                CollateralWorkflowServiceGrpc.newBlockingStub(channel),
                CollateralWorkflowServiceGrpc.newFutureStub(channel),
                LoanWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterApprovalServiceGrpc.newBlockingStub(channel),
                RiskScenarioWorkflowServiceGrpc.newBlockingStub(channel),
                StressTestApprovalServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newStub(channel),
                TripServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ReinstateSanctionRequest.getDefaultInstance());
        var response = application.reinstateSanction(request);
        var sent0 = channel.request("acme.compliance.v1.ControlCalculationService/ExplainControlCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent0, "control", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent0, "control", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent0, "control", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.compliance.v1.ControlValidationService/PreviewControl");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/ExplainControlCalculation"), "control", "owner", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/PreviewControl"), "control", "block", "reason"),
                ProbeSupport.value(sent2, "reason"));
        var sent3 = channel.request("acme.audit.v1.RetentionPolicySyncService/PullRetentionPolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "retention_policy", "block", "notify_email"),
                ProbeSupport.value(sent3, "retention_policy", "block", "notify_email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent3, "retention_policy", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/PreviewControl"), "control", "block", "reason"),
                ProbeSupport.value(sent3, "retention_policy", "block", "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/ExplainControlCalculation"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlCalculationService/ExplainControlCalculation"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlValidationService/PreviewControl"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "retention_policy", "owner", "email"),
                ProbeSupport.value(response, "sanction", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "retention_policy", "audit", "updated_by"),
                ProbeSupport.value(response, "sanction", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicySyncService/PullRetentionPolicies"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicySyncService/PullRetentionPolicies"), "retention_policy", "owner", "user_id"),
                ProbeSupport.value(response, "sanction", "children", "children", "manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "sanction_id"),
                ProbeSupport.value(response, "sanction", "sanction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(response, "sanction", "manager", "user_id"));
    }
}

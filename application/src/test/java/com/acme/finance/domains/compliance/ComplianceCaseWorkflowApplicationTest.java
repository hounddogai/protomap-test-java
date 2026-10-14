package com.acme.finance.domains.compliance;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ScheduleRetentionPolicyExportRequest;
import com.acme.audit.v1.ScheduleRetentionPolicyExportResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionRequest;
import com.acme.authorization.v1.ExplainAccessPolicyDecisionResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.AdvanceComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.AdvanceComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.CancelComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.CancelComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.CompareAttestationOptionsRequest;
import com.acme.compliance.v1.CompareAttestationOptionsResponse;
import com.acme.compliance.v1.CompareComplianceCaseOptionsRequest;
import com.acme.compliance.v1.CompareComplianceCaseOptionsResponse;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseChannelOrigin;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.ControlCalculationServiceGrpc;
import com.acme.compliance.v1.InspectComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.InspectComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.ListComplianceCaseWorkflowsRequest;
import com.acme.compliance.v1.ListComplianceCaseWorkflowsResponse;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.PauseComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.QuoteControlRequest;
import com.acme.compliance.v1.QuoteControlResponse;
import com.acme.compliance.v1.RecallComplianceCaseRequest;
import com.acme.compliance.v1.RecallComplianceCaseResponse;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.ResumeComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.RetryComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.RetryComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.ShareAttestationReportRequest;
import com.acme.compliance.v1.ShareAttestationReportResponse;
import com.acme.compliance.v1.StartComplianceCaseWorkflowRequest;
import com.acme.compliance.v1.StartComplianceCaseWorkflowResponse;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forms.v1.ExportFormsRequest;
import com.acme.forms.v1.ExportFormsResponse;
import com.acme.forms.v1.FormQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListPendingPoliciesRequest;
import com.acme.insurance.v1.ListPendingPoliciesResponse;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.notifications.v1.NormalizeNotificationRequest;
import com.acme.notifications.v1.NormalizeNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.ResumeExposureWorkflowRequest;
import com.acme.risk.v1.ResumeExposureWorkflowResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ComplianceCaseWorkflowApplicationTest {
    @Test
    void resumeComplianceCaseWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ComplianceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAttestation(String tenant, Attestation value) {}
            public void saveComplianceCase(String tenant, ComplianceCase value) {}
            public void saveControl(String tenant, Control value) {}
            public void saveSanction(String tenant, Sanction value) {}
        };
        var application = new ComplianceCaseWorkflowApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                ComplianceCaseApprovalServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ControlApprovalServiceGrpc.newBlockingStub(channel),
                ControlCalculationServiceGrpc.newBlockingStub(channel),
                FormQueryServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                new ComplianceCaseWorkflowRequestMapperImpl(),
                repository,
                ExposureWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ResumeComplianceCaseWorkflowRequest.getDefaultInstance());
        var response = application.resumeComplianceCaseWorkflow(request);
        var sent0 = channel.request("acme.compliance.v1.ControlApprovalService/SubmitControl");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "comments", "created_at"),
                ProbeSupport.value(sent0, "control", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "comments", "author_email"),
                ProbeSupport.value(sent0, "details", "approver", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "comments", "comment_id"),
                ProbeSupport.value(sent0, "details", "comment"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "requester", "email"),
                ProbeSupport.value(sent0, "control", "owner", "email"));
        var sent1 = channel.request("acme.compliance.v1.AttestationReportingService/ShareAttestationReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        var sent2 = channel.request("acme.notifications.v1.NotificationValidationService/NormalizeNotification");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlApprovalService/SubmitControl"), "control", "owner", "user_id"),
                ProbeSupport.value(sent2, "notification", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "customer", "number"),
                ProbeSupport.value(sent2, "notification", "customer", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "assignee", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationReportingService/ShareAttestationReport"), "attestation", "counterparty", "display_name"),
                ProbeSupport.value(sent2, "notification", "assignee", "display_name"));
        var sent3 = channel.request("acme.audit.v1.RetentionPolicyExportService/ScheduleRetentionPolicyExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlApprovalService/SubmitControl"), "control", "block", "reason"),
                ProbeSupport.value(sent3, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationReportingService/ShareAttestationReport"), "attestation", "audit", "etag"),
                ProbeSupport.value(response, "compliance_case", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationValidationService/NormalizeNotification"), "notification", "tags"),
                ProbeSupport.value(response, "compliance_case", "tags"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ScheduleRetentionPolicyExport"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case_id"),
                ProbeSupport.value(response, "compliance_case", "compliance_case_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "compliance_case", "requester"),
                ProbeSupport.value(response, "compliance_case", "requester"));
    }
}

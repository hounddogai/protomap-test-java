package com.acme.finance.domains.payroll;

import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.PreviewUserRequest;
import com.acme.identity.v1.PreviewUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.insurance.v1.SyncUnderwritingsRequest;
import com.acme.insurance.v1.SyncUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingImportRequest;
import com.acme.insurance.v1.ValidateUnderwritingImportResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.notifications.v1.CloneNotificationRequest;
import com.acme.notifications.v1.CloneNotificationResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.payroll.v1.AdvancePayRunWorkflowRequest;
import com.acme.payroll.v1.AdvancePayRunWorkflowResponse;
import com.acme.payroll.v1.CancelPayRunWorkflowRequest;
import com.acme.payroll.v1.CancelPayRunWorkflowResponse;
import com.acme.payroll.v1.CommentPayStubApprovalRequest;
import com.acme.payroll.v1.CommentPayStubApprovalResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DelegatePayRunApprovalRequest;
import com.acme.payroll.v1.DelegatePayRunApprovalResponse;
import com.acme.payroll.v1.GenerateDeductionReportRequest;
import com.acme.payroll.v1.GenerateDeductionReportResponse;
import com.acme.payroll.v1.InspectPayRunWorkflowRequest;
import com.acme.payroll.v1.InspectPayRunWorkflowResponse;
import com.acme.payroll.v1.ListPayRunWorkflowsRequest;
import com.acme.payroll.v1.ListPayRunWorkflowsResponse;
import com.acme.payroll.v1.PausePayRunWorkflowRequest;
import com.acme.payroll.v1.PausePayRunWorkflowResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunApprovalServiceGrpc;
import com.acme.payroll.v1.PayRunBankSettlement;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubApprovalServiceGrpc;
import com.acme.payroll.v1.PayStubAttachment;
import com.acme.payroll.v1.PayStubCardSettlement;
import com.acme.payroll.v1.PayStubLifecycleServiceGrpc;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.RecallPayStubRequest;
import com.acme.payroll.v1.RecallPayStubResponse;
import com.acme.payroll.v1.ReinstateDeductionRequest;
import com.acme.payroll.v1.ReinstateDeductionResponse;
import com.acme.payroll.v1.ResolvePayStubsRequest;
import com.acme.payroll.v1.ResolvePayStubsResponse;
import com.acme.payroll.v1.ResumePayRunWorkflowRequest;
import com.acme.payroll.v1.ResumePayRunWorkflowResponse;
import com.acme.payroll.v1.RetryPayRunWorkflowRequest;
import com.acme.payroll.v1.RetryPayRunWorkflowResponse;
import com.acme.payroll.v1.SchedulePayStubExpiryRequest;
import com.acme.payroll.v1.SchedulePayStubExpiryResponse;
import com.acme.payroll.v1.SignalPayRunWorkflowRequest;
import com.acme.payroll.v1.SignalPayRunWorkflowResponse;
import com.acme.payroll.v1.StartPayRunWorkflowRequest;
import com.acme.payroll.v1.StartPayRunWorkflowResponse;
import com.acme.payroll.v1.SubmitPayRunRequest;
import com.acme.payroll.v1.SubmitPayRunResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.returns.v1.CloneReturnRequestRequest;
import com.acme.returns.v1.CloneReturnRequestResponse;
import com.acme.returns.v1.ReturnRequest;
import com.acme.returns.v1.ReturnRequestBankSettlement;
import com.acme.returns.v1.ReturnRequestServiceGrpc;
import com.acme.risk.v1.CommentRiskScenarioApprovalRequest;
import com.acme.risk.v1.CommentRiskScenarioApprovalResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.venues.v1.UpdateVenueRequest;
import com.acme.venues.v1.UpdateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
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
import org.springframework.stereotype.Service;

class PayRunWorkflowApplicationTest {
    @Test
    void pausePayRunWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayRunWorkflowApplication(
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UserQueryServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                DeductionLifecycleServiceGrpc.newBlockingStub(channel),
                DeductionReportingServiceGrpc.newBlockingStub(channel),
                PayRunApprovalServiceGrpc.newBlockingStub(channel),
                PayStubApprovalServiceGrpc.newBlockingStub(channel),
                PayStubQueryServiceGrpc.newBlockingStub(channel),
                repository,
                ReturnRequestServiceGrpc.newBlockingStub(channel),
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(PausePayRunWorkflowRequest.getDefaultInstance());
        var response = application.pausePayRunWorkflow(request);
        var sent0 = channel.request("acme.insurance.v1.UnderwritingSyncService/SyncUnderwritings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "audit", "updated_by"),
                ProbeSupport.value(sent0, "underwriting", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent0, "underwriting", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "audit", "created_at"),
                ProbeSupport.value(sent0, "underwriting", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "underwriting", "requester", "preferred_channel"));
        var sent1 = channel.request("acme.identity.v1.UserValidationService/PreviewUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/SyncUnderwritings"), "underwriting", "requester", "name"),
                ProbeSupport.value(sent1, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/SyncUnderwritings"), "underwriting", "requester", "email"),
                ProbeSupport.value(sent1, "user", "relationship_manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "billing_address", "region"),
                ProbeSupport.value(sent1, "user", "primary_address", "region"));
        var sent2 = channel.request("acme.ledger.v1.FiscalPeriodNotificationService/NotifyFiscalPeriodWatchers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/SyncUnderwritings"), "underwriting", "assignee", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/PreviewUser"), "user", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/PreviewUser"), "user", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/SyncUnderwritings"), "underwriting", "requester", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        var sent3 = channel.request("acme.payroll.v1.PayStubQueryService/ResolvePayStubs");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/SyncUnderwritings"), "underwriting", "assignee", "display_name"),
                ProbeSupport.value(response, "pay_run", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.UnderwritingSyncService/SyncUnderwritings"), "underwriting", "sla", "due_at"),
                ProbeSupport.value(response, "pay_run", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/PreviewUser"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/PreviewUser"), "user", "audit", "updated_by"),
                ProbeSupport.value(response, "pay_run", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodNotificationService/NotifyFiscalPeriodWatchers"), "fiscal_period", "location", "locality"),
                ProbeSupport.value(response, "pay_run", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodNotificationService/NotifyFiscalPeriodWatchers"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run_id"),
                ProbeSupport.value(response, "pay_run", "pay_run_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "metadata"),
                ProbeSupport.value(response, "pay_run", "metadata"));
    }
}

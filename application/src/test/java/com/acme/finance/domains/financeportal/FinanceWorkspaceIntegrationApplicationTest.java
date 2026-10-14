package com.acme.finance.domains.financeportal;

import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsApprovalServiceGrpc;
import com.acme.accounting.v1.RejectChartOfAccountsRequest;
import com.acme.accounting.v1.RejectChartOfAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordMeasurement;
import com.acme.customers.v1.ConsentRecordSyncServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.HouseholdReportingServiceGrpc;
import com.acme.customers.v1.ListHouseholdReportsRequest;
import com.acme.customers.v1.ListHouseholdReportsResponse;
import com.acme.customers.v1.ListPreferenceReportsRequest;
import com.acme.customers.v1.ListPreferenceReportsResponse;
import com.acme.customers.v1.ReconcileConsentRecordsRequest;
import com.acme.customers.v1.ReconcileConsentRecordsResponse;
import com.acme.datahub.v1.BatchGetWorkspacesRequest;
import com.acme.datahub.v1.BatchGetWorkspacesResponse;
import com.acme.datahub.v1.WorkspaceServiceGrpc;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCardSettlement;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.expenses.v1.RetryMileageClaimWorkflowRequest;
import com.acme.expenses.v1.RetryMileageClaimWorkflowResponse;
import com.acme.expenses.v1.SignalMileageClaimWorkflowRequest;
import com.acme.expenses.v1.SignalMileageClaimWorkflowResponse;
import com.acme.expenses.v1.StartMileageClaimWorkflowRequest;
import com.acme.expenses.v1.StartMileageClaimWorkflowResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxCardSettlement;
import com.acme.financeportal.v1.ApprovalInboxWorkflowServiceGrpc;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.ConnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceRequest;
import com.acme.financeportal.v1.DisconnectFinanceWorkspaceSourceResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceComment;
import com.acme.financeportal.v1.FinanceWorkspaceSla;
import com.acme.financeportal.v1.IngestFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.IngestFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsRequest;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSourcesResponse;
import com.acme.financeportal.v1.RelayFinanceWorkspaceMessagesRequest;
import com.acme.financeportal.v1.RelayFinanceWorkspaceMessagesResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionRequest;
import com.acme.financeportal.v1.TestFinanceWorkspaceConnectionResponse;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.RevertJournalEntryRequest;
import com.acme.ledger.v1.RevertJournalEntryResponse;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.lending.v1.CommentLoanApplicationApprovalRequest;
import com.acme.lending.v1.CommentLoanApplicationApprovalResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionBlockAction;
import com.acme.payroll.v1.DeductionWorkflowServiceGrpc;
import com.acme.payroll.v1.PauseDeductionWorkflowRequest;
import com.acme.payroll.v1.PauseDeductionWorkflowResponse;
import com.acme.suppliers.v1.ListPendingSupplierRisksRequest;
import com.acme.suppliers.v1.ListPendingSupplierRisksResponse;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingLineTax;
import com.acme.suppliers.v1.SupplierRatingWorkflowServiceGrpc;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class FinanceWorkspaceIntegrationApplicationTest {
    @Test
    void disconnectFinanceWorkspaceSourcePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new FinanceWorkspaceIntegrationApplicationEvents(WorkspaceServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FinanceWorkspaceIntegrationApplication.RelayFinanceWorkspaceMessagesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FinanceWorkspaceIntegrationApplication(
                ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordSyncServiceGrpc.newBlockingStub(channel),
                HouseholdReportingServiceGrpc.newBlockingStub(channel),
                events,
                MileageClaimWorkflowServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceIntegrationRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                DeductionWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRatingWorkflowServiceGrpc.newBlockingStub(channel),
                SupplierRiskApprovalServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(DisconnectFinanceWorkspaceSourceRequest.getDefaultInstance());
        var response = application.disconnectFinanceWorkspaceSource(request);
        var sent0 = channel.request("acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "received_at"),
                ProbeSupport.value(sent0, "consent_record", "measurements", "observed_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "message_id"),
                ProbeSupport.value(sent0, "consent_record", "source"));
        var sent1 = channel.request("acme.lending.v1.LoanApplicationApprovalService/CommentLoanApplicationApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords"), "consent_record", "attributes"),
                ProbeSupport.value(sent1, "loan_application", "lines", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords"), "consent_record", "subject", "display_name"),
                ProbeSupport.value(sent1, "details", "approver", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords"), "outcome", "effective_at"),
                ProbeSupport.value(sent1, "details", "effective_at"));
        var sent2 = channel.request("acme.customers.v1.PreferenceReportingService/ListPreferenceReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationApprovalService/CommentLoanApplicationApproval"), "loan_application", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationApprovalService/CommentLoanApplicationApproval"), "loan_application", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords"), "outcome", "reference_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationApprovalService/CommentLoanApplicationApproval"), "result", "details"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.customers.v1.HouseholdReportingService/ListHouseholdReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationApprovalService/CommentLoanApplicationApproval"), "loan_application", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords"), "outcome", "effective_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordSyncService/ReconcileConsentRecords"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanApplicationApprovalService/CommentLoanApplicationApproval"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceReportingService/ListPreferenceReports"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdReportingService/ListHouseholdReports"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace_id"),
                ProbeSupport.value(response, "finance_workspace", "finance_workspace_id"));
    }

    @Test
    void ingestFinanceWorkspaceEventsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new FinanceWorkspaceIntegrationApplicationEvents(WorkspaceServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FinanceWorkspaceIntegrationApplication.RelayFinanceWorkspaceMessagesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FinanceWorkspaceIntegrationApplication(
                ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordSyncServiceGrpc.newBlockingStub(channel),
                HouseholdReportingServiceGrpc.newBlockingStub(channel),
                events,
                MileageClaimWorkflowServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceIntegrationRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                DeductionWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRatingWorkflowServiceGrpc.newBlockingStub(channel),
                SupplierRiskApprovalServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(IngestFinanceWorkspaceEventsRequest.getDefaultInstance());
        var response = application.ingestFinanceWorkspaceEvents(request);
        var sent0 = channel.request("acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "header", "format"),
                ProbeSupport.value(sent0, "details", "format"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "custom_fields"),
                ProbeSupport.value(sent0, "details", "filters"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "baggage"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.financeportal.v1.ApprovalInboxWorkflowService/ListApprovalInboxWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "history", "changed_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "customer", "supplier_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses"), "result", "details"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.expenses.v1.MileageClaimWorkflowService/RetryMileageClaimWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_by"),
                ProbeSupport.value(sent2, "mileage_claim", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesQueryService/SummarizeMetricSerieses"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimWorkflowService/RetryMileageClaimWorkflow"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.MileageClaimWorkflowService/RetryMileageClaimWorkflow"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new FinanceWorkspaceIntegrationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<IngestFinanceWorkspaceEventsResponse> observer = new StreamObserver<>() {
            public void onNext(IngestFinanceWorkspaceEventsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.ingestFinanceWorkspaceEvents(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void relayFinanceWorkspaceMessagesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new FinanceWorkspaceIntegrationApplicationEvents(WorkspaceServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FinanceWorkspaceIntegrationApplication.RelayFinanceWorkspaceMessagesStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FinanceWorkspaceIntegrationApplication(
                ChartOfAccountsApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ConsentRecordSyncServiceGrpc.newBlockingStub(channel),
                HouseholdReportingServiceGrpc.newBlockingStub(channel),
                events,
                MileageClaimWorkflowServiceGrpc.newBlockingStub(channel),
                ApprovalInboxWorkflowServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceIntegrationRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                DeductionWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRatingWorkflowServiceGrpc.newBlockingStub(channel),
                SupplierRiskApprovalServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(RelayFinanceWorkspaceMessagesRequest.getDefaultInstance());
        var response = application.relayFinanceWorkspaceMessages(request);
        var sent0 = channel.request("acme.datahub.v1.WorkspaceService/BatchGetWorkspaces");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        var sent1 = channel.request("acme.financeportal.v1.ApprovalInboxWorkflowService/StartApprovalInboxWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "created_at"),
                ProbeSupport.value(sent1, "approval_inbox", "audit", "created_at"));
        var sent2 = channel.request("acme.financeportal.v1.ApprovalInboxWorkflowService/SignalApprovalInboxWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "comments", "created_at"),
                ProbeSupport.value(sent2, "approval_inbox", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "comments", "author_email"),
                ProbeSupport.value(sent2, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.datahub.v1.WorkspaceService/BatchGetWorkspaces"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.datahub.v1.WorkspaceService/BatchGetWorkspaces"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxWorkflowService/StartApprovalInboxWorkflow"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxWorkflowService/StartApprovalInboxWorkflow"), "approval_inbox", "audit", "created_at"),
                ProbeSupport.value(response, "finance_workspace", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "status"),
                ProbeSupport.value(response, "finance_workspace", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "requester"),
                ProbeSupport.value(response, "finance_workspace", "requester"));
        var endpoint = new FinanceWorkspaceIntegrationEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<RelayFinanceWorkspaceMessagesResponse> observer = new StreamObserver<>() {
            public void onNext(RelayFinanceWorkspaceMessagesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.relayFinanceWorkspaceMessages(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new FinanceWorkspaceIntegrationApplicationEvents(WorkspaceServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<BatchGetWorkspacesResponse>();
        listener.on(new FinanceWorkspaceIntegrationApplication.RelayFinanceWorkspaceMessagesStage0Requested(ProbeSupport.sample(BatchGetWorkspacesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}

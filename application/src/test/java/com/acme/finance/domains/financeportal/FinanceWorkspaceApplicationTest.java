package com.acme.finance.domains.financeportal;

import com.acme.analytics.v1.AnalyticsEventQueryServiceGrpc;
import com.acme.analytics.v1.SummarizeAnalyticsEventsRequest;
import com.acme.analytics.v1.SummarizeAnalyticsEventsResponse;
import com.acme.budgeting.v1.CommentVarianceApprovalRequest;
import com.acme.budgeting.v1.CommentVarianceApprovalResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.customers.v1.GenerateHouseholdReportRequest;
import com.acme.customers.v1.GenerateHouseholdReportResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdReportingServiceGrpc;
import com.acme.customers.v1.HouseholdWorkflowServiceGrpc;
import com.acme.customers.v1.ListPreferenceConflictsRequest;
import com.acme.customers.v1.ListPreferenceConflictsResponse;
import com.acme.customers.v1.PreferenceImportServiceGrpc;
import com.acme.customers.v1.PreferenceSyncServiceGrpc;
import com.acme.customers.v1.RetryHouseholdWorkflowRequest;
import com.acme.customers.v1.RetryHouseholdWorkflowResponse;
import com.acme.customers.v1.ValidatePreferenceImportRequest;
import com.acme.customers.v1.ValidatePreferenceImportResponse;
import com.acme.expenses.v1.AdvanceReceiptWorkflowRequest;
import com.acme.expenses.v1.AdvanceReceiptWorkflowResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyBlockAction;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptWorkflowServiceGrpc;
import com.acme.expenses.v1.ReconcileExpensePoliciesRequest;
import com.acme.expenses.v1.ReconcileExpensePoliciesResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxNotificationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxQueryServiceGrpc;
import com.acme.financeportal.v1.BatchCreateFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BatchCreateFinanceWorkspacesResponse;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesRequest;
import com.acme.financeportal.v1.BatchGetFinanceWorkspacesResponse;
import com.acme.financeportal.v1.CloneFinanceWorkspaceRequest;
import com.acme.financeportal.v1.CloneFinanceWorkspaceResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistIntegrationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CountApprovalInboxesRequest;
import com.acme.financeportal.v1.CountApprovalInboxesResponse;
import com.acme.financeportal.v1.CreateFinanceWorkspaceRequest;
import com.acme.financeportal.v1.CreateFinanceWorkspaceResponse;
import com.acme.financeportal.v1.DeleteFinanceWorkspaceRequest;
import com.acme.financeportal.v1.DeleteFinanceWorkspaceResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceComment;
import com.acme.financeportal.v1.GetFinanceWorkspaceRequest;
import com.acme.financeportal.v1.GetFinanceWorkspaceResponse;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsRequest;
import com.acme.financeportal.v1.ListCloseChecklistWorkflowsResponse;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersRequest;
import com.acme.financeportal.v1.NotifyApprovalInboxWatchersResponse;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsRequest;
import com.acme.financeportal.v1.ReplayCloseChecklistEventsResponse;
import com.acme.financeportal.v1.SearchFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SearchFinanceWorkspacesResponse;
import com.acme.financeportal.v1.SuggestApprovalInboxesRequest;
import com.acme.financeportal.v1.SuggestApprovalInboxesResponse;
import com.acme.financeportal.v1.WatchFinanceWorkspacesRequest;
import com.acme.financeportal.v1.WatchFinanceWorkspacesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.notifications.v1.FindNotificationsRequest;
import com.acme.notifications.v1.FindNotificationsResponse;
import com.acme.notifications.v1.NotificationQueryServiceGrpc;
import com.acme.treasury.v1.ImportLiquidityForecastsRequest;
import com.acme.treasury.v1.ImportLiquidityForecastsResponse;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.venues.v1.SuggestVenuesRequest;
import com.acme.venues.v1.SuggestVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class FinanceWorkspaceApplicationTest {
    @Test
    void createFinanceWorkspacePreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new FinanceWorkspaceApplication(
                AnalyticsEventQueryServiceGrpc.newBlockingStub(channel),
                VarianceApprovalServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ControlWorkflowServiceGrpc.newBlockingStub(channel),
                HouseholdReportingServiceGrpc.newBlockingStub(channel),
                HouseholdWorkflowServiceGrpc.newBlockingStub(channel),
                PreferenceImportServiceGrpc.newBlockingStub(channel),
                PreferenceSyncServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ReceiptWorkflowServiceGrpc.newBlockingStub(channel),
                ApprovalInboxNotificationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxQueryServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceRequestMapperImpl(),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                LiquidityForecastImportServiceGrpc.newStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CreateFinanceWorkspaceRequest.getDefaultInstance());
        var response = application.createFinanceWorkspace(request);
        var sent0 = channel.request("acme.customers.v1.PreferenceSyncService/ListPreferenceConflicts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "assignee", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.analytics.v1.AnalyticsEventQueryService/SummarizeAnalyticsEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceSyncService/ListPreferenceConflicts"), "result", "details"),
                ProbeSupport.value(sent1, "details", "filters"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "custom_fields"),
                ProbeSupport.value(sent1, "annotations"));
        var sent2 = channel.request("acme.financeportal.v1.ApprovalInboxQueryService/CountApprovalInboxes");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "customer", "supplier_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.compliance.v1.ControlWorkflowService/StartControlWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "requester", "email"),
                ProbeSupport.value(sent3, "control", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "audit", "updated_by"),
                ProbeSupport.value(sent3, "control", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "audit", "created_at"),
                ProbeSupport.value(sent3, "control", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "customer", "display_name"),
                ProbeSupport.value(sent3, "control", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceSyncService/ListPreferenceConflicts"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.analytics.v1.AnalyticsEventQueryService/SummarizeAnalyticsEvents"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.analytics.v1.AnalyticsEventQueryService/SummarizeAnalyticsEvents"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ControlWorkflowService/StartControlWorkflow"), "control", "priority"),
                ProbeSupport.value(response, "finance_workspace", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "status"),
                ProbeSupport.value(response, "finance_workspace", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "finance_workspace", "assignee"),
                ProbeSupport.value(response, "finance_workspace", "assignee"));
    }

    @Test
    void watchFinanceWorkspacesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new FinanceWorkspaceApplication(
                AnalyticsEventQueryServiceGrpc.newBlockingStub(channel),
                VarianceApprovalServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ControlWorkflowServiceGrpc.newBlockingStub(channel),
                HouseholdReportingServiceGrpc.newBlockingStub(channel),
                HouseholdWorkflowServiceGrpc.newBlockingStub(channel),
                PreferenceImportServiceGrpc.newBlockingStub(channel),
                PreferenceSyncServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ReceiptWorkflowServiceGrpc.newBlockingStub(channel),
                ApprovalInboxNotificationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxQueryServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceRequestMapperImpl(),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                LiquidityForecastImportServiceGrpc.newStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(WatchFinanceWorkspacesRequest.getDefaultInstance());
        var response = application.watchFinanceWorkspaces(request);
        var sent0 = channel.request("acme.venues.v1.VenueQueryService/SuggestVenues");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent0, "filter", "query"));
        var sent1 = channel.request("acme.customers.v1.PreferenceImportService/ValidatePreferenceImport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent1, "details", "contact", "phone"));
        var sent2 = channel.request("acme.expenses.v1.ExpensePolicySyncService/ReconcileExpensePolicies");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceImportService/ValidatePreferenceImport"), "preference", "block", "reason"),
                ProbeSupport.value(sent2, "expense_policy", "block", "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceImportService/ValidatePreferenceImport"), "preference", "audit", "etag"),
                ProbeSupport.value(sent2, "expense_policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceImportService/ValidatePreferenceImport"), "preference", "owner", "email"),
                ProbeSupport.value(sent2, "expense_policy", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceImportService/ValidatePreferenceImport"), "preference", "block", "notify_email"),
                ProbeSupport.value(sent2, "expense_policy", "block", "notify_email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueQueryService/SuggestVenues"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceImportService/ValidatePreferenceImport"), "preference", "audit", "etag"),
                ProbeSupport.value(response, "finance_workspace", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceImportService/ValidatePreferenceImport"), "preference", "audit", "created_at"),
                ProbeSupport.value(response, "finance_workspace", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicySyncService/ReconcileExpensePolicies"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "finance_workspace", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(response, "finance_workspace", "requester", "phone"));
        var endpoint = new FinanceWorkspaceEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchFinanceWorkspacesResponse> observer = new StreamObserver<>() {
            public void onNext(WatchFinanceWorkspacesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchFinanceWorkspaces(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateFinanceWorkspacesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new FinanceWorkspaceApplication(
                AnalyticsEventQueryServiceGrpc.newBlockingStub(channel),
                VarianceApprovalServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                ControlWorkflowServiceGrpc.newBlockingStub(channel),
                HouseholdReportingServiceGrpc.newBlockingStub(channel),
                HouseholdWorkflowServiceGrpc.newBlockingStub(channel),
                PreferenceImportServiceGrpc.newBlockingStub(channel),
                PreferenceSyncServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ReceiptWorkflowServiceGrpc.newBlockingStub(channel),
                ApprovalInboxNotificationServiceGrpc.newBlockingStub(channel),
                ApprovalInboxQueryServiceGrpc.newBlockingStub(channel),
                CloseChecklistIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                new FinanceWorkspaceRequestMapperImpl(),
                NotificationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                LiquidityForecastImportServiceGrpc.newStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateFinanceWorkspacesRequest.getDefaultInstance());
        var response = application.batchCreateFinanceWorkspaces(request);
        var sent0 = channel.request("acme.customers.v1.HouseholdWorkflowService/RetryHouseholdWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "comments", "created_at"),
                ProbeSupport.value(sent0, "household", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "comments", "author_email"),
                ProbeSupport.value(sent0, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "customer", "display_name"),
                ProbeSupport.value(sent0, "household", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "assignee", "email"),
                ProbeSupport.value(sent0, "household", "children", "children", "manager", "email"));
        var sent1 = channel.request("acme.financeportal.v1.ApprovalInboxQueryService/SuggestApprovalInboxes");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdWorkflowService/RetryHouseholdWorkflow"), "household", "location", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.notifications.v1.NotificationQueryService/FindNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdWorkflowService/RetryHouseholdWorkflow"), "household", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "sla", "due_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "customer", "supplier_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.ApprovalInboxQueryService/SuggestApprovalInboxes"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationQueryService/FindNotifications"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new FinanceWorkspaceEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateFinanceWorkspacesResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateFinanceWorkspacesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateFinanceWorkspaces(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}

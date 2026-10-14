package com.acme.finance.domains.financeportal;

import com.acme.assets.v1.CancelDepreciationWorkflowRequest;
import com.acme.assets.v1.CancelDepreciationWorkflowResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.UnsubscribeRetentionPolicyUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentBankSettlement;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.NormalizeDocumentRequest;
import com.acme.documents.v1.NormalizeDocumentResponse;
import com.acme.edgegateway.v1.RouteBindingQueryServiceGrpc;
import com.acme.edgegateway.v1.SummarizeRouteBindingsRequest;
import com.acme.edgegateway.v1.SummarizeRouteBindingsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.ApprovalInboxLine;
import com.acme.financeportal.v1.ApprovalInboxLineTax;
import com.acme.financeportal.v1.BatchGetApprovalInboxesRequest;
import com.acme.financeportal.v1.BatchGetApprovalInboxesResponse;
import com.acme.financeportal.v1.CloneApprovalInboxRequest;
import com.acme.financeportal.v1.CloneApprovalInboxResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistNotificationServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistQueryServiceGrpc;
import com.acme.financeportal.v1.CloseChecklistRef;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CreateApprovalInboxRequest;
import com.acme.financeportal.v1.CreateApprovalInboxResponse;
import com.acme.financeportal.v1.DeleteApprovalInboxRequest;
import com.acme.financeportal.v1.DeleteApprovalInboxResponse;
import com.acme.financeportal.v1.ExportCloseChecklistsRequest;
import com.acme.financeportal.v1.ExportCloseChecklistsResponse;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceNotificationServiceGrpc;
import com.acme.financeportal.v1.GetApprovalInboxRequest;
import com.acme.financeportal.v1.GetApprovalInboxResponse;
import com.acme.financeportal.v1.ListApprovalInboxSourcesRequest;
import com.acme.financeportal.v1.ListApprovalInboxSourcesResponse;
import com.acme.financeportal.v1.ListApprovalInboxesRequest;
import com.acme.financeportal.v1.ListApprovalInboxesResponse;
import com.acme.financeportal.v1.SearchApprovalInboxesRequest;
import com.acme.financeportal.v1.SearchApprovalInboxesResponse;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.StartCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesRequest;
import com.acme.financeportal.v1.SubscribeCloseChecklistUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.UpdateApprovalInboxRequest;
import com.acme.financeportal.v1.UpdateApprovalInboxResponse;
import com.acme.financeportal.v1.WatchApprovalInboxesRequest;
import com.acme.financeportal.v1.WatchApprovalInboxesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.CountEInvoiceTotalsRequest;
import com.acme.invoicing.v1.CountEInvoiceTotalsResponse;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexComponent;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierContractApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierContractRequest;
import com.acme.suppliers.v1.EscalateSupplierContractResponse;
import com.acme.suppliers.v1.ListSupplierWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierWorkflowsResponse;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractAttachment;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.acme.venues.v1.UpdateVenueRequest;
import com.acme.venues.v1.UpdateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.acme.webbff.v1.PageCompositionQueryServiceGrpc;
import com.acme.webbff.v1.ResolvePageCompositionsRequest;
import com.acme.webbff.v1.ResolvePageCompositionsResponse;
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
import org.springframework.stereotype.Service;

class ApprovalInboxApplicationTest {
    @Test
    void createApprovalInboxPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new ApprovalInboxApplication(
                DepreciationWorkflowServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                RouteBindingQueryServiceGrpc.newBlockingStub(channel),
                ApprovalInboxIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistQueryServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceNotificationServiceGrpc.newBlockingStub(channel),
                EInvoiceReportingServiceGrpc.newFutureStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel),
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel),
                SupplierWorkflowServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(CreateApprovalInboxRequest.getDefaultInstance());
        var response = application.createApprovalInbox(request);
        var sent0 = channel.request("acme.documents.v1.DocumentValidationService/NormalizeDocument");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "bank", "iban"),
                ProbeSupport.value(sent0, "document", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "billing_address", "lines"),
                ProbeSupport.value(sent0, "document", "billing_address", "lines"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierWorkflowService/ListSupplierWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "approval_inbox_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/NormalizeDocument"), "document", "metadata"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.financeportal.v1.CloseChecklistQueryService/ExportCloseChecklists");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/NormalizeDocument"), "document", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/NormalizeDocument"), "document", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "counterparty", "close_checklist_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.ledger.v1.LedgerAccountNotificationService/SendLedgerAccountTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent3, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/NormalizeDocument"), "document", "metadata"),
                ProbeSupport.value(sent3, "details", "variables"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierWorkflowService/ListSupplierWorkflows"), "result", "details"),
                ProbeSupport.value(sent3, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/NormalizeDocument"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.documents.v1.DocumentValidationService/NormalizeDocument"), "document", "number"),
                ProbeSupport.value(response, "approval_inbox", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierWorkflowService/ListSupplierWorkflows"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountNotificationService/SendLedgerAccountTestNotice"), "ledger_account", "audit", "created_at"),
                ProbeSupport.value(response, "approval_inbox", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "issued_at"),
                ProbeSupport.value(response, "approval_inbox", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "number"),
                ProbeSupport.value(response, "approval_inbox", "counterparty", "number"));
    }

    @Test
    void watchApprovalInboxesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var application = new ApprovalInboxApplication(
                DepreciationWorkflowServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                RouteBindingQueryServiceGrpc.newBlockingStub(channel),
                ApprovalInboxIntegrationServiceGrpc.newBlockingStub(channel),
                CloseChecklistQueryServiceGrpc.newBlockingStub(channel),
                CloseChecklistWorkflowServiceGrpc.newBlockingStub(channel),
                FinanceWorkspaceNotificationServiceGrpc.newBlockingStub(channel),
                EInvoiceReportingServiceGrpc.newFutureStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel),
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel),
                SupplierWorkflowServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(WatchApprovalInboxesRequest.getDefaultInstance());
        var response = application.watchApprovalInboxes(request);
        var sent0 = channel.request("acme.financeportal.v1.CloseChecklistNotificationService/SubscribeCloseChecklistUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "details", "recipient", "name"));
        var sent1 = channel.request("acme.financeportal.v1.CloseChecklistWorkflowService/StartCloseChecklistWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistNotificationService/SubscribeCloseChecklistUpdates"), "close_checklist", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "close_checklist", "counterparty", "display_name"));
        var sent2 = channel.request("acme.invoicing.v1.EInvoiceReportingService/CountEInvoiceTotals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/StartCloseChecklistWorkflow"), "close_checklist", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistNotificationService/SubscribeCloseChecklistUpdates"), "close_checklist", "billing_address", "postal_code"),
                ProbeSupport.value(response, "approval_inbox", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistNotificationService/SubscribeCloseChecklistUpdates"), "close_checklist", "total"),
                ProbeSupport.value(response, "approval_inbox", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.financeportal.v1.CloseChecklistWorkflowService/StartCloseChecklistWorkflow"), "close_checklist", "audit", "etag"),
                ProbeSupport.value(response, "approval_inbox", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.EInvoiceReportingService/CountEInvoiceTotals"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "approval_inbox", "billing_address", "region"));
        var endpoint = new ApprovalInboxEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchApprovalInboxesResponse> observer = new StreamObserver<>() {
            public void onNext(WatchApprovalInboxesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchApprovalInboxes(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}

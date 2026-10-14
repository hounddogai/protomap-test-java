package com.acme.finance.domains.financeportal;

import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationWorkflowServiceGrpc;
import com.acme.accounting.v1.StartAllocationWorkflowRequest;
import com.acme.accounting.v1.StartAllocationWorkflowResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCardSettlement;
import com.acme.assets.v1.AssetTransferWorkflowServiceGrpc;
import com.acme.assets.v1.PauseAssetTransferWorkflowRequest;
import com.acme.assets.v1.PauseAssetTransferWorkflowResponse;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ListAuditTrailConflictsRequest;
import com.acme.audit.v1.ListAuditTrailConflictsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerApprovalServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerSegmentSyncServiceGrpc;
import com.acme.customers.v1.PullCustomerSegmentsRequest;
import com.acme.customers.v1.PullCustomerSegmentsResponse;
import com.acme.customers.v1.RecallCustomerRequest;
import com.acme.customers.v1.RecallCustomerResponse;
import com.acme.driverapp.v1.DriverHomeQueryServiceGrpc;
import com.acme.driverapp.v1.ResolveDriverHomesRequest;
import com.acme.driverapp.v1.ResolveDriverHomesResponse;
import com.acme.edgegateway.v1.BatchGetRouteBindingsRequest;
import com.acme.edgegateway.v1.BatchGetRouteBindingsResponse;
import com.acme.edgegateway.v1.RouteBindingServiceGrpc;
import com.acme.employeeportal.v1.FindPortalHomesRequest;
import com.acme.employeeportal.v1.FindPortalHomesResponse;
import com.acme.employeeportal.v1.PortalHomeQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.AdvanceApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.CancelApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistRef;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.InspectApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsRequest;
import com.acme.financeportal.v1.ListApprovalInboxWorkflowsResponse;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.PauseApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.ResumeApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.RetryApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.SignalApprovalInboxWorkflowResponse;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowRequest;
import com.acme.financeportal.v1.StartApprovalInboxWorkflowResponse;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.SubmitCoverageRequest;
import com.acme.insurance.v1.SubmitCoverageResponse;
import com.acme.ledger.v1.CreateLedgerAccountRequest;
import com.acme.ledger.v1.CreateLedgerAccountResponse;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.SendJournalEntryTestNoticeRequest;
import com.acme.ledger.v1.SendJournalEntryTestNoticeResponse;
import com.acme.ledger.v1.ShareFiscalPeriodExportRequest;
import com.acme.ledger.v1.ShareFiscalPeriodExportResponse;
import com.acme.lending.v1.EscalateLoanRequest;
import com.acme.lending.v1.EscalateLoanResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationAttachment;
import com.acme.lending.v1.LoanApplicationWorkflowServiceGrpc;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanBankFunding;
import com.acme.lending.v1.StartLoanApplicationWorkflowRequest;
import com.acme.lending.v1.StartLoanApplicationWorkflowResponse;
import com.acme.patientapp.v1.BrowsePatientHomesRequest;
import com.acme.patientapp.v1.BrowsePatientHomesResponse;
import com.acme.patientapp.v1.PatientHomeQueryServiceGrpc;
import com.acme.risk.v1.EscalateRiskScenarioRequest;
import com.acme.risk.v1.EscalateRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.suppliers.v1.RecallSupplierRatingRequest;
import com.acme.suppliers.v1.RecallSupplierRatingResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingLine;
import com.acme.suppliers.v1.SupplierRatingLineTax;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ApprovalInboxWorkflowApplicationTest {
    @Test
    void advanceApprovalInboxWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new FinanceportalRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveApprovalInbox(String tenant, ApprovalInbox value) {}
            public void saveCloseChecklist(String tenant, CloseChecklist value) {}
            public void saveFinanceWorkspace(String tenant, FinanceWorkspace value) {}
        };
        var listener = new ApprovalInboxWorkflowApplicationEvents(AuditTrailSyncServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ApprovalInboxWorkflowApplication.PauseApprovalInboxWorkflowStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ApprovalInboxWorkflowApplication(
                AllocationWorkflowServiceGrpc.newBlockingStub(channel),
                AssetTransferWorkflowServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                CustomerApprovalServiceGrpc.newBlockingStub(channel),
                CustomerSegmentSyncServiceGrpc.newBlockingStub(channel),
                DriverHomeQueryServiceGrpc.newBlockingStub(channel),
                RouteBindingServiceGrpc.newBlockingStub(channel),
                PortalHomeQueryServiceGrpc.newBlockingStub(channel),
                events,
                UserQueryServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                CoverageApprovalServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                LoanApplicationWorkflowServiceGrpc.newBlockingStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                PatientHomeQueryServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioApprovalServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierRatingApprovalServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(AdvanceApprovalInboxWorkflowRequest.getDefaultInstance());
        var response = application.advanceApprovalInboxWorkflow(request);
        var sent0 = channel.request("acme.customers.v1.CustomerSegmentSyncService/PullCustomerSegments");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "lines", "tax", "amount"),
                ProbeSupport.value(sent0, "customer_segment", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "lines", "description"),
                ProbeSupport.value(sent0, "customer_segment", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "customer_segment", "owner", "display_name"));
        var sent1 = channel.request("acme.patientapp.v1.PatientHomeQueryService/BrowsePatientHomes");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentSyncService/PullCustomerSegments"), "result", "details"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierRatingApprovalService/RecallSupplierRating");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentSyncService/PullCustomerSegments"), "customer_segment", "discount", "amount"),
                ProbeSupport.value(sent2, "supplier_rating", "lines", "tax", "amount"));
        var sent3 = channel.request("acme.ledger.v1.FiscalPeriodExportService/ShareFiscalPeriodExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentSyncService/PullCustomerSegments"), "customer_segment", "owner", "email"),
                ProbeSupport.value(sent3, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent3, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.patientapp.v1.PatientHomeQueryService/BrowsePatientHomes"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingApprovalService/RecallSupplierRating"), "supplier_rating", "billing_address", "region"),
                ProbeSupport.value(response, "approval_inbox", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingApprovalService/RecallSupplierRating"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox_id"),
                ProbeSupport.value(response, "approval_inbox", "approval_inbox_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "approval_inbox", "due_at"),
                ProbeSupport.value(response, "approval_inbox", "due_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ApprovalInboxWorkflowApplicationEvents(AuditTrailSyncServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListAuditTrailConflictsResponse>();
        listener.on(new ApprovalInboxWorkflowApplication.PauseApprovalInboxWorkflowStage0Requested(ProbeSupport.sample(ListAuditTrailConflictsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}

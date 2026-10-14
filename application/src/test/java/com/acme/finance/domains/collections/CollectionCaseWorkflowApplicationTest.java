package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.ListRetentionPolicyConflictsRequest;
import com.acme.audit.v1.ListRetentionPolicyConflictsResponse;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.RecallMandateRequest;
import com.acme.banking.v1.RecallMandateResponse;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowRequest;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowResponse;
import com.acme.collections.v1.BrowseCollectionCasesRequest;
import com.acme.collections.v1.BrowseCollectionCasesResponse;
import com.acme.collections.v1.CancelCollectionCaseWorkflowRequest;
import com.acme.collections.v1.CancelCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.CollectionCaseSla;
import com.acme.collections.v1.CreateCollectionCaseRequest;
import com.acme.collections.v1.CreateCollectionCaseResponse;
import com.acme.collections.v1.DeleteCollectionCaseRequest;
import com.acme.collections.v1.DeleteCollectionCaseResponse;
import com.acme.collections.v1.InspectCollectionCaseWorkflowRequest;
import com.acme.collections.v1.InspectCollectionCaseWorkflowResponse;
import com.acme.collections.v1.LintWriteOffsRequest;
import com.acme.collections.v1.LintWriteOffsResponse;
import com.acme.collections.v1.ListCollectionCaseWorkflowsRequest;
import com.acme.collections.v1.ListCollectionCaseWorkflowsResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PauseCollectionCaseWorkflowRequest;
import com.acme.collections.v1.PauseCollectionCaseWorkflowResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.collections.v1.ResumeCollectionCaseWorkflowRequest;
import com.acme.collections.v1.ResumeCollectionCaseWorkflowResponse;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.collections.v1.SignalCollectionCaseWorkflowRequest;
import com.acme.collections.v1.SignalCollectionCaseWorkflowResponse;
import com.acme.collections.v1.StartCollectionCaseWorkflowRequest;
import com.acme.collections.v1.StartCollectionCaseWorkflowResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ApprovePremiumRequest;
import com.acme.insurance.v1.ApprovePremiumResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageApprovalServiceGrpc;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.DelegateCoverageApprovalRequest;
import com.acme.insurance.v1.DelegateCoverageApprovalResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumApprovalServiceGrpc;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.insurance.v1.StreamPremiumMetricsRequest;
import com.acme.insurance.v1.StreamPremiumMetricsResponse;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryCardSettlement;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountCardFunding;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
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

class CollectionCaseWorkflowApplicationTest {
    @Test
    void startCollectionCaseWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new CollectionsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveCollectionCase(String tenant, CollectionCase value) {}
            public void savePaymentPromise(String tenant, PaymentPromise value) {}
            public void saveWriteOff(String tenant, WriteOff value) {}
        };
        var application = new CollectionCaseWorkflowApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                MandateApprovalServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseApprovalServiceGrpc.newBlockingStub(channel),
                CollectionCaseServiceGrpc.newBlockingStub(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                CoverageApprovalServiceGrpc.newBlockingStub(channel),
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                PremiumApprovalServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(StartCollectionCaseWorkflowRequest.getDefaultInstance());
        var response = application.startCollectionCaseWorkflow(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailHistoryService/ListAuditTrailVersions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "comments", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "comments", "comment_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "parameters"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.ledger.v1.JournalEntryHistoryService/AnnotateJournalEntryVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "comments", "created_at"),
                ProbeSupport.value(sent1, "journal_entry", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "comments", "author_email"),
                ProbeSupport.value(sent1, "details", "author_email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "audit", "etag"),
                ProbeSupport.value(sent1, "journal_entry", "audit", "etag"));
        var sent2 = channel.request("acme.collections.v1.CollectionCaseQueryService/BrowseCollectionCases");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryHistoryService/AnnotateJournalEntryVersion"), "journal_entry", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryHistoryService/AnnotateJournalEntryVersion"), "journal_entry", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        var sent3 = channel.request("acme.banking.v1.MandateApprovalService/RecallMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "comments", "author_email"),
                ProbeSupport.value(sent3, "mandate", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "comments", "created_at"),
                ProbeSupport.value(sent3, "mandate", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.JournalEntryHistoryService/AnnotateJournalEntryVersion"), "journal_entry", "billing_address", "recipient"),
                ProbeSupport.value(sent3, "mandate", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "audit", "etag"),
                ProbeSupport.value(sent3, "mandate", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseQueryService/BrowseCollectionCases"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseQueryService/BrowseCollectionCases"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case_id"),
                ProbeSupport.value(response, "collection_case", "collection_case_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "collection_case", "requester"),
                ProbeSupport.value(response, "collection_case", "requester"));
    }
}

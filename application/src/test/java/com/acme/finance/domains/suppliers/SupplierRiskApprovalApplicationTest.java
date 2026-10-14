package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.assets.v1.ListDepreciationWorkflowsRequest;
import com.acme.assets.v1.ListDepreciationWorkflowsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainPreferenceDecisionRequest;
import com.acme.customers.v1.ExplainPreferenceDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.ScheduleRepaymentReportRequest;
import com.acme.lending.v1.ScheduleRepaymentReportResponse;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.suppliers.v1.ApproveSupplierRatingRequest;
import com.acme.suppliers.v1.ApproveSupplierRatingResponse;
import com.acme.suppliers.v1.ApproveSupplierRiskRequest;
import com.acme.suppliers.v1.ApproveSupplierRiskResponse;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.CommentSupplierRiskApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierRiskApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierRiskRequest;
import com.acme.suppliers.v1.EscalateSupplierRiskResponse;
import com.acme.suppliers.v1.GetSupplierRiskApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierRiskApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.ListPendingSupplierRisksRequest;
import com.acme.suppliers.v1.ListPendingSupplierRisksResponse;
import com.acme.suppliers.v1.RecallSupplierRatingRequest;
import com.acme.suppliers.v1.RecallSupplierRatingResponse;
import com.acme.suppliers.v1.RecallSupplierRiskRequest;
import com.acme.suppliers.v1.RecallSupplierRiskResponse;
import com.acme.suppliers.v1.RejectSupplierRiskRequest;
import com.acme.suppliers.v1.RejectSupplierRiskResponse;
import com.acme.suppliers.v1.SubmitSupplierRatingRequest;
import com.acme.suppliers.v1.SubmitSupplierRatingResponse;
import com.acme.suppliers.v1.SubmitSupplierRiskRequest;
import com.acme.suppliers.v1.SubmitSupplierRiskResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskSla;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class SupplierRiskApprovalApplicationTest {
    @Test
    void submitSupplierRiskPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var application = new SupplierRiskApprovalApplication(
                DepreciationWorkflowServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                PreferenceValidationServiceGrpc.newBlockingStub(channel),
                JournalEntryNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureWorkflowServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel),
                SupplierContractQueryServiceGrpc.newFutureStub(channel),
                SupplierRatingApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SubmitSupplierRiskRequest.getDefaultInstance());
        var response = application.submitSupplierRisk(request);
        var sent0 = channel.request("acme.risk.v1.ExposureWorkflowService/CancelExposureWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_risk", "requester", "phone"),
                ProbeSupport.value(sent0, "details", "notify", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_risk", "requester", "name"),
                ProbeSupport.value(sent0, "details", "notify", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_risk", "assignee", "email"),
                ProbeSupport.value(sent0, "exposure", "holder", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "display_name"),
                ProbeSupport.value(sent0, "exposure", "holder", "display_name"));
        var sent1 = channel.request("acme.ledger.v1.PostingBatchNotificationService/ListPostingBatchSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/CancelExposureWorkflow"), "exposure", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/CancelExposureWorkflow"), "exposure", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierRatingApprovalService/ApproveSupplierRating");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent2, "supplier_rating", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_risk", "history", "changed_at"),
                ProbeSupport.value(sent2, "supplier_rating", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_risk", "assignee", "user_id"),
                ProbeSupport.value(sent2, "details", "approver", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_risk", "audit", "updated_by"),
                ProbeSupport.value(sent2, "supplier_rating", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureWorkflowService/CancelExposureWorkflow"), "exposure", "contact", "email"),
                ProbeSupport.value(response, "supplier_risk", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/ListPostingBatchSubscriptions"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchNotificationService/ListPostingBatchSubscriptions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_risk_id"),
                ProbeSupport.value(response, "supplier_risk", "supplier_risk_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_risk", "status"),
                ProbeSupport.value(response, "supplier_risk", "status"));
    }
}

package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.CheckVarianceEligibilityRequest;
import com.acme.budgeting.v1.CheckVarianceEligibilityResponse;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.GetVarianceRulesRequest;
import com.acme.budgeting.v1.GetVarianceRulesResponse;
import com.acme.budgeting.v1.PreviewBudgetLineRequest;
import com.acme.budgeting.v1.PreviewBudgetLineResponse;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.QuoteBudgetRequest;
import com.acme.budgeting.v1.QuoteBudgetResponse;
import com.acme.budgeting.v1.RecalculateBudgetsRequest;
import com.acme.budgeting.v1.RecalculateBudgetsResponse;
import com.acme.budgeting.v1.ScoreVarianceRequest;
import com.acme.budgeting.v1.ScoreVarianceResponse;
import com.acme.budgeting.v1.ValidateVarianceRequest;
import com.acme.budgeting.v1.ValidateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.datasets.v1.CreateDatasetRequest;
import com.acme.datasets.v1.CreateDatasetResponse;
import com.acme.datasets.v1.Dataset;
import com.acme.datasets.v1.DatasetComponent;
import com.acme.datasets.v1.DatasetServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ScoreCoverageRequest;
import com.acme.insurance.v1.ScoreCoverageResponse;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.recommendations.v1.BatchGetRecommendationSetsRequest;
import com.acme.recommendations.v1.BatchGetRecommendationSetsResponse;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import com.google.protobuf.Any;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class VarianceValidationApplicationTest {
    @Test
    void explainVarianceDecisionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BudgetingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBudget(String tenant, Budget value) {}
            public void saveBudgetLine(String tenant, BudgetLine value) {}
            public void saveForecast(String tenant, Forecast value) {}
            public void saveVariance(String tenant, Variance value) {}
        };
        var listener = new VarianceValidationApplicationEvents(BudgetLineValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof VarianceValidationApplication.ExplainVarianceDecisionStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new VarianceValidationApplication(
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetCalculationServiceGrpc.newBlockingStub(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                DatasetServiceGrpc.newBlockingStub(channel),
                events,
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                JournalEntryHistoryServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationValidationServiceGrpc.newBlockingStub(channel),
                RecommendationSetServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ExplainVarianceDecisionRequest.getDefaultInstance());
        var response = application.explainVarianceDecision(request);
        var sent0 = channel.request("acme.audit.v1.AuditEventHistoryService/RecordAuditEventVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "assignee", "display_name"),
                ProbeSupport.value(sent0, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "customer", "number"),
                ProbeSupport.value(sent0, "audit_event", "subject", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "requester", "email"),
                ProbeSupport.value(sent0, "details", "author_email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        var sent1 = channel.request("acme.datasets.v1.DatasetService/CreateDataset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "audit", "etag"),
                ProbeSupport.value(sent1, "dataset", "audit", "etag"));
        var sent2 = channel.request("acme.budgeting.v1.BudgetLineValidationService/PreviewBudgetLine");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "audit", "updated_by"),
                ProbeSupport.value(sent2, "budget_line", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.datasets.v1.DatasetService/CreateDataset"), "dataset", "components", "quantity"),
                ProbeSupport.value(sent2, "budget_line", "lines", "quantity"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetCalculationService/RecalculateBudgets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RecordAuditEventVersion"), "audit_event", "subject", "display_name"),
                ProbeSupport.value(sent3, "budget", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "assignee", "email"),
                ProbeSupport.value(sent3, "budget", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "preferred_channel"),
                ProbeSupport.value(sent3, "budget", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RecordAuditEventVersion"), "audit_event", "subject", "display_name"),
                ProbeSupport.value(response, "variance", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/RecordAuditEventVersion"), "audit_event", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.datasets.v1.DatasetService/CreateDataset"), "dataset", "audit", "updated_at"),
                ProbeSupport.value(response, "variance", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineValidationService/PreviewBudgetLine"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetCalculationService/RecalculateBudgets"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance_id"),
                ProbeSupport.value(response, "variance", "variance_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "variance", "requester"),
                ProbeSupport.value(response, "variance", "requester"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new VarianceValidationApplicationEvents(BudgetLineValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<PreviewBudgetLineResponse>();
        listener.on(new VarianceValidationApplication.ExplainVarianceDecisionStage2Requested(ProbeSupport.sample(PreviewBudgetLineRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}

package com.acme.finance.domains.insurance;

import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentLine;
import com.acme.documents.v1.DocumentLineTax;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.featureflags.v1.ExplainFeatureFlagDecisionRequest;
import com.acme.featureflags.v1.ExplainFeatureFlagDecisionResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.EstimatePolicyRequest;
import com.acme.insurance.v1.EstimatePolicyResponse;
import com.acme.insurance.v1.ExplainPolicyCalculationRequest;
import com.acme.insurance.v1.ExplainPolicyCalculationResponse;
import com.acme.insurance.v1.GetPolicyRatesRequest;
import com.acme.insurance.v1.GetPolicyRatesResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.QuotePolicyRequest;
import com.acme.insurance.v1.QuotePolicyResponse;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.insurance.v1.SimulatePolicyRequest;
import com.acme.insurance.v1.SimulatePolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsRequest;
import com.acme.ledger.v1.ListFiscalPeriodSubscriptionsResponse;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersRequest;
import com.acme.ledger.v1.NotifyFiscalPeriodWatchersResponse;
import com.acme.ledger.v1.NotifyPostingBatchWatchersRequest;
import com.acme.ledger.v1.NotifyPostingBatchWatchersResponse;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesRequest;
import com.acme.ledger.v1.SubscribeFiscalPeriodUpdatesResponse;
import com.acme.ledger.v1.UnsubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.UnsubscribePostingBatchUpdatesResponse;
import com.acme.messaging.v1.BatchGetConversationsRequest;
import com.acme.messaging.v1.BatchGetConversationsResponse;
import com.acme.messaging.v1.ConversationServiceGrpc;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.UpdateMetricSeriesRequest;
import com.acme.metrics.v1.UpdateMetricSeriesResponse;
import com.acme.prescriptions.v1.Prescription;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.PrescriptionLine;
import com.acme.prescriptions.v1.PrescriptionLineTax;
import com.acme.prescriptions.v1.QuotePrescriptionRequest;
import com.acme.prescriptions.v1.QuotePrescriptionResponse;
import com.acme.recruiting.v1.CountJobOpeningsRequest;
import com.acme.recruiting.v1.CountJobOpeningsResponse;
import com.acme.recruiting.v1.JobOpeningQueryServiceGrpc;
import com.acme.search.v1.BatchGetSearchIndexesRequest;
import com.acme.search.v1.BatchGetSearchIndexesResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.secrets.v1.SearchSecretsRequest;
import com.acme.secrets.v1.SearchSecretsResponse;
import com.acme.secrets.v1.SecretServiceGrpc;
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

class PolicyCalculationApplicationTest {
    @Test
    void quotePolicyPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InsuranceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveClaim(String tenant, Claim value) {}
            public void saveCoverage(String tenant, Coverage value) {}
            public void savePolicy(String tenant, Policy value) {}
            public void savePremium(String tenant, Premium value) {}
            public void saveUnderwriting(String tenant, Underwriting value) {}
        };
        var application = new PolicyCalculationApplication(
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                DocumentValidationServiceGrpc.newBlockingStub(channel),
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                ConversationServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                PrescriptionCalculationServiceGrpc.newBlockingStub(channel),
                JobOpeningQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel),
                SecretServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(QuotePolicyRequest.getDefaultInstance());
        var response = application.quotePolicy(request);
        var sent0 = channel.request("acme.ledger.v1.PostingBatchValidationService/PreviewPostingBatch");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "limits", "amount"),
                ProbeSupport.value(sent0, "posting_batch", "lines", "amount"));
        var sent1 = channel.request("acme.identity.v1.UserService/UpdateUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "contact", "phone"),
                ProbeSupport.value(sent1, "user", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/PreviewPostingBatch"), "posting_batch", "audit", "updated_by"),
                ProbeSupport.value(sent1, "user", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/PreviewPostingBatch"), "posting_batch", "audit", "created_by"),
                ProbeSupport.value(sent1, "user", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.PostingBatchValidationService/PreviewPostingBatch"), "posting_batch", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "user", "primary_address", "recipient"));
        var sent2 = channel.request("acme.messaging.v1.ConversationService/BatchGetConversations");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        var sent3 = channel.request("acme.search.v1.SearchIndexService/UpdateSearchIndex");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "limits", "name"),
                ProbeSupport.value(sent3, "search_index", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/UpdateUser"), "user", "audit", "etag"),
                ProbeSupport.value(response, "policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.messaging.v1.ConversationService/BatchGetConversations"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy_id"),
                ProbeSupport.value(response, "policy", "policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "policy", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
    }
}

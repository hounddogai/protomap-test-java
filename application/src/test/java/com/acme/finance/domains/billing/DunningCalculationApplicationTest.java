package com.acme.finance.domains.billing;

import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.SummarizeFixedAssetsRequest;
import com.acme.assets.v1.SummarizeFixedAssetsResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CalculateDunningRequest;
import com.acme.billing.v1.CalculateDunningResponse;
import com.acme.billing.v1.CalculatePaymentTermRequest;
import com.acme.billing.v1.CalculatePaymentTermResponse;
import com.acme.billing.v1.CompareDunningOptionsRequest;
import com.acme.billing.v1.CompareDunningOptionsResponse;
import com.acme.billing.v1.ComparePaymentTermOptionsRequest;
import com.acme.billing.v1.ComparePaymentTermOptionsResponse;
import com.acme.billing.v1.CreditNote;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningSla;
import com.acme.billing.v1.EstimateDunningRequest;
import com.acme.billing.v1.EstimateDunningResponse;
import com.acme.billing.v1.ExplainDunningCalculationRequest;
import com.acme.billing.v1.ExplainDunningCalculationResponse;
import com.acme.billing.v1.ExplainInvoiceDecisionRequest;
import com.acme.billing.v1.ExplainInvoiceDecisionResponse;
import com.acme.billing.v1.GetDunningRatesRequest;
import com.acme.billing.v1.GetDunningRatesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintInvoicesRequest;
import com.acme.billing.v1.LintInvoicesResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.billing.v1.QuoteDunningRequest;
import com.acme.billing.v1.QuoteDunningResponse;
import com.acme.billing.v1.RecalculateDunningsRequest;
import com.acme.billing.v1.RecalculateDunningsResponse;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.SimulateDunningRequest;
import com.acme.billing.v1.SimulateDunningResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.SuggestBudgetLinesRequest;
import com.acme.budgeting.v1.SuggestBudgetLinesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSla;
import com.acme.budgeting.v1.VarianceStatusChange;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.collections.v1.ExplainWriteOffDecisionRequest;
import com.acme.collections.v1.ExplainWriteOffDecisionResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.ScoreFeatureFlagRequest;
import com.acme.featureflags.v1.ScoreFeatureFlagResponse;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.CountMetricSeriesesRequest;
import com.acme.metrics.v1.CountMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.SearchPayRunsRequest;
import com.acme.payroll.v1.SearchPayRunsResponse;
import com.acme.search.v1.ExportSearchIndexesRequest;
import com.acme.search.v1.ExportSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.UpdateVenueRequest;
import com.acme.venues.v1.UpdateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
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

class DunningCalculationApplicationTest {
    @Test
    void quoteDunningPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new BillingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBillingAccount(String tenant, BillingAccount value) {}
            public void saveCreditNote(String tenant, CreditNote value) {}
            public void saveDunning(String tenant, Dunning value) {}
            public void saveInvoice(String tenant, Invoice value) {}
            public void savePaymentTerm(String tenant, PaymentTerm value) {}
        };
        var application = new DunningCalculationApplication(
                FixedAssetQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                BillingAccountValidationServiceGrpc.newBlockingStub(channel),
                InvoiceCalculationServiceGrpc.newBlockingStub(channel),
                InvoiceValidationServiceGrpc.newBlockingStub(channel),
                PaymentTermCalculationServiceGrpc.newFutureStub(channel),
                BudgetLineQueryServiceGrpc.newBlockingStub(channel),
                VarianceValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffValidationServiceGrpc.newBlockingStub(channel),
                new DunningCalculationRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                PayRunServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SearchIndexQueryServiceGrpc.newBlockingStub(channel),
                VenueServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(QuoteDunningRequest.getDefaultInstance());
        var response = application.quoteDunning(request);
        var sent0 = channel.request("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "audit", "created_by"),
                ProbeSupport.value(sent0, "variance", "audit", "created_by"));
        var sent1 = channel.request("acme.billing.v1.InvoiceValidationService/ExplainInvoiceDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "variance", "requester", "name"),
                ProbeSupport.value(sent1, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "assignee", "display_name"),
                ProbeSupport.value(sent1, "invoice", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "requester", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "contact", "preferred_channel"));
        var sent2 = channel.request("acme.billing.v1.BillingAccountValidationService/PreviewBillingAccount");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "audit", "created_at"),
                ProbeSupport.value(sent2, "billing_account", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ExplainInvoiceDecision"), "invoice", "bank", "bic"),
                ProbeSupport.value(sent2, "billing_account", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "variance", "customer", "number"),
                ProbeSupport.value(sent2, "billing_account", "holder", "number"));
        var sent3 = channel.request("acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/PreviewBillingAccount"), "billing_account", "audit", "created_at"),
                ProbeSupport.value(sent3, "feature_flag", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "country_code"),
                ProbeSupport.value(sent3, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ExplainInvoiceDecision"), "invoice", "billing_address", "lines"),
                ProbeSupport.value(sent3, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceValidationService/ExplainVarianceDecision"), "variance", "customer", "display_name"),
                ProbeSupport.value(response, "dunning", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ExplainInvoiceDecision"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.InvoiceValidationService/ExplainInvoiceDecision"), "invoice", "audit", "updated_at"),
                ProbeSupport.value(response, "dunning", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.billing.v1.BillingAccountValidationService/PreviewBillingAccount"), "billing_account", "contact", "preferred_channel"),
                ProbeSupport.value(response, "dunning", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag"), "feature_flag", "discount", "amount"),
                ProbeSupport.value(response, "breakdown", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/ScoreFeatureFlag"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning_id"),
                ProbeSupport.value(response, "dunning", "dunning_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "dunning", "custom_fields"),
                ProbeSupport.value(response, "dunning", "custom_fields"));
    }
}

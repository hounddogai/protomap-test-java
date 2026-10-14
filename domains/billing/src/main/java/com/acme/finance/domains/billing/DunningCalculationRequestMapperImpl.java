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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class DunningCalculationRequestMapperImpl implements DunningCalculationRequestMapper {
    @Override
    public ExplainVarianceDecisionRequest compareDunningOptionsStage0(CompareDunningOptionsRequest request) {
        return ExplainVarianceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainVarianceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDunning().getRequester().getPhone())
                                .build())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDunning().getCustomer().getDisplayName())
                                .setEmail(request.getDunning().getRequester().getEmail())
                                .build())
                        .setSla(VarianceSla.newBuilder()
                                .setBreached(request.getDunning().getSla().getBreached())
                                .build())
                        .build())
                .build();
    }
}

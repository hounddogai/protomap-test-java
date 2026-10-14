package com.acme.finance.domains.payroll;

import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.ListAccessPoliciesRequest;
import com.acme.authorization.v1.ListAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.BrowseForumsRequest;
import com.acme.community.v1.BrowseForumsResponse;
import com.acme.community.v1.ForumQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.licensing.v1.ListSoftwareLicensesRequest;
import com.acme.licensing.v1.ListSoftwareLicensesResponse;
import com.acme.licensing.v1.SoftwareLicenseServiceGrpc;
import com.acme.metrics.v1.ListMetricSeriesesRequest;
import com.acme.metrics.v1.ListMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.payroll.v1.BatchCreatePayRunsRequest;
import com.acme.payroll.v1.BatchCreatePayRunsResponse;
import com.acme.payroll.v1.BatchGetPayRunsRequest;
import com.acme.payroll.v1.BatchGetPayRunsResponse;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.CalculateTaxWithholdingRequest;
import com.acme.payroll.v1.CalculateTaxWithholdingResponse;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityRequest;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityResponse;
import com.acme.payroll.v1.ClonePayRunRequest;
import com.acme.payroll.v1.ClonePayRunResponse;
import com.acme.payroll.v1.CreatePayRunRequest;
import com.acme.payroll.v1.CreatePayRunResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.ExplainPayStubDecisionRequest;
import com.acme.payroll.v1.ExplainPayStubDecisionResponse;
import com.acme.payroll.v1.GetPayRunRatesRequest;
import com.acme.payroll.v1.GetPayRunRatesResponse;
import com.acme.payroll.v1.GetPayRunRequest;
import com.acme.payroll.v1.GetPayRunResponse;
import com.acme.payroll.v1.LintTaxWithholdingsRequest;
import com.acme.payroll.v1.LintTaxWithholdingsResponse;
import com.acme.payroll.v1.ListPayRunsRequest;
import com.acme.payroll.v1.ListPayRunsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunQueryServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubBankSettlement;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.ScoreDeductionRequest;
import com.acme.payroll.v1.ScoreDeductionResponse;
import com.acme.payroll.v1.SearchPayRunsRequest;
import com.acme.payroll.v1.SearchPayRunsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingDiscountAction;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.UpdatePayRunRequest;
import com.acme.payroll.v1.UpdatePayRunResponse;
import com.acme.procurement.v1.PreviewRfqRequest;
import com.acme.procurement.v1.PreviewRfqResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class PayRunRequestMapperImpl implements PayRunRequestMapper {
    @Override
    public SuggestJournalEntriesRequest updatePayRunStage0(UpdatePayRunRequest request) {
        return SuggestJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayRun().getDueAt())
                        .putAllLabels(request.getPayRun().getMetadataMap())
                        .setOwnerId(request.getPayRun().getPayRunId())
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
    }

    @Override
    public BrowsePayRunsRequest clonePayRunStage1(ClonePayRunRequest request, NormalizeDeductionResponse reply0) {
        return BrowsePayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowsePayRunsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayRun().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getDeduction().getParametersMap())
                        .setOwnerId(request.getPayRun().getCounterparty().getDeductionId())
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}

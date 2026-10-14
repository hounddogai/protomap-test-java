package com.acme.finance.domains.payroll;

import com.acme.assets.v1.BrowseDepreciationsRequest;
import com.acme.assets.v1.BrowseDepreciationsResponse;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailLine;
import com.acme.audit.v1.AuditTrailLineTax;
import com.acme.audit.v1.FindAuditEventsRequest;
import com.acme.audit.v1.FindAuditEventsResponse;
import com.acme.audit.v1.RecordRetentionPolicyVersionRequest;
import com.acme.audit.v1.RecordRetentionPolicyVersionResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyRef;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.SuggestJournalEntriesRequest;
import com.acme.ledger.v1.SuggestJournalEntriesResponse;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.payroll.v1.CalculatePayStubRequest;
import com.acme.payroll.v1.CalculatePayStubResponse;
import com.acme.payroll.v1.CalculateTaxWithholdingRequest;
import com.acme.payroll.v1.CalculateTaxWithholdingResponse;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityRequest;
import com.acme.payroll.v1.CheckTaxWithholdingEligibilityResponse;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsRequest;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimateDeductionRequest;
import com.acme.payroll.v1.EstimateDeductionResponse;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingCalculationResponse;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionRequest;
import com.acme.payroll.v1.ExplainTaxWithholdingDecisionResponse;
import com.acme.payroll.v1.LintTaxWithholdingsRequest;
import com.acme.payroll.v1.LintTaxWithholdingsResponse;
import com.acme.payroll.v1.NormalizeTaxWithholdingRequest;
import com.acme.payroll.v1.NormalizeTaxWithholdingResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PreviewTaxWithholdingRequest;
import com.acme.payroll.v1.PreviewTaxWithholdingResponse;
import com.acme.payroll.v1.ScoreTaxWithholdingRequest;
import com.acme.payroll.v1.ScoreTaxWithholdingResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingCondition;
import com.acme.payroll.v1.ValidateTaxWithholdingRequest;
import com.acme.payroll.v1.ValidateTaxWithholdingResponse;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.BatchCreatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
public final class TaxWithholdingValidationRequestMapperImpl implements TaxWithholdingValidationRequestMapper {
    @Override
    public SuggestJournalEntriesRequest normalizeTaxWithholdingStage1(NormalizeTaxWithholdingRequest request, CheckPayRunEligibilityResponse reply0) {
        return SuggestJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestJournalEntriesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPayRun().getIssuedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getTaxWithholding().getOwner().getUserId())
                        .setRegion(reply0.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}

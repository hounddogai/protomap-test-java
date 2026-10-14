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
import com.acme.finance.testing.ProbeSupport;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class TaxWithholdingValidationApplicationTest {
    @Test
    void scoreTaxWithholdingPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new TaxWithholdingValidationApplicationEvents(TaxWithholdingCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof TaxWithholdingValidationApplication.PreviewTaxWithholdingStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new TaxWithholdingValidationApplication(
                DepreciationQueryServiceGrpc.newFutureStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                new TaxWithholdingValidationRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionServiceGrpc.newStub(channel),
                repository,
                SearchIndexServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ScoreTaxWithholdingRequest.getDefaultInstance());
        var response = application.scoreTaxWithholding(request);
        var sent0 = channel.request("acme.payroll.v1.TaxWithholdingCalculationService/CalculateTaxWithholding");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "conditions", "negate"),
                ProbeSupport.value(sent0, "tax_withholding", "conditions", "negate"));
        var sent1 = channel.request("acme.payroll.v1.DeductionCalculationService/EstimateDeduction");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "conditions", "value"),
                ProbeSupport.value(sent1, "deduction", "conditions", "value"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "conditions", "operator"),
                ProbeSupport.value(sent1, "deduction", "conditions", "operator"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "conditions", "attribute"),
                ProbeSupport.value(sent1, "deduction", "conditions", "attribute"));
        var sent2 = channel.request("acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/CalculateTaxWithholding"), "tax_withholding", "owner", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/CalculateTaxWithholding"), "tax_withholding", "parameters"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.suppliers.v1.SupplierService/ListSuppliers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesQueryService/BrowseMetricSerieses"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/ListSuppliers"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/ListSuppliers"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding_id"),
                ProbeSupport.value(response, "tax_withholding", "tax_withholding_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "tax_withholding", "name"),
                ProbeSupport.value(response, "tax_withholding", "name"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new TaxWithholdingValidationApplicationEvents(TaxWithholdingCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ExplainTaxWithholdingCalculationResponse>();
        listener.on(new TaxWithholdingValidationApplication.PreviewTaxWithholdingStage0Requested(ProbeSupport.sample(ExplainTaxWithholdingCalculationRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}

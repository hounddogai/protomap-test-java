package com.acme.finance.domains.procurement;

import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditEventVersionsRequest;
import com.acme.audit.v1.ListAuditEventVersionsResponse;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BatchGetBudgetsRequest;
import com.acme.budgeting.v1.BatchGetBudgetsResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.RecalculateVariancesRequest;
import com.acme.budgeting.v1.RecalculateVariancesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.featureflags.v1.BatchGetFeatureFlagsRequest;
import com.acme.featureflags.v1.BatchGetFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.BrowseLoansRequest;
import com.acme.lending.v1.BrowseLoansResponse;
import com.acme.lending.v1.LoanQueryServiceGrpc;
import com.acme.metrics.v1.BatchCreateMetricSeriesesRequest;
import com.acme.metrics.v1.BatchCreateMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.procurement.v1.CalculateGoodsReceiptRequest;
import com.acme.procurement.v1.CalculateGoodsReceiptResponse;
import com.acme.procurement.v1.CalculatePurchaseOrderRequest;
import com.acme.procurement.v1.CalculatePurchaseOrderResponse;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsRequest;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.EstimateGoodsReceiptRequest;
import com.acme.procurement.v1.EstimateGoodsReceiptResponse;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationRequest;
import com.acme.procurement.v1.ExplainGoodsReceiptCalculationResponse;
import com.acme.procurement.v1.GetGoodsReceiptRatesRequest;
import com.acme.procurement.v1.GetGoodsReceiptRatesResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderBankSettlement;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.QuoteGoodsReceiptRequest;
import com.acme.procurement.v1.QuoteGoodsReceiptResponse;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.RecalculateGoodsReceiptsRequest;
import com.acme.procurement.v1.RecalculateGoodsReceiptsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.SimulateGoodsReceiptRequest;
import com.acme.procurement.v1.SimulateGoodsReceiptResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
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

class GoodsReceiptCalculationApplicationTest {
    @Test
    void compareGoodsReceiptOptionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var listener = new GoodsReceiptCalculationApplicationEvents(AuditEventHistoryServiceGrpc.newStub(channel), BudgetLineCalculationServiceGrpc.newStub(channel), VarianceCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof GoodsReceiptCalculationApplication.QuoteGoodsReceiptStage1Requested command) {
                listener.on(command);
            }
            else if (event instanceof GoodsReceiptCalculationApplication.CompareGoodsReceiptOptionsStage3Requested command) {
                listener.on(command);
            }
            else if (event instanceof GoodsReceiptCalculationApplication.RecalculateGoodsReceiptsStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new GoodsReceiptCalculationApplication(
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetServiceGrpc.newStub(channel),
                BudgetServiceGrpc.newBlockingStub(channel),
                ComplianceCaseQueryServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagServiceGrpc.newBlockingStub(channel),
                LoanQueryServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newStub(channel),
                PurchaseOrderCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CompareGoodsReceiptOptionsRequest.getDefaultInstance());
        var response = application.compareGoodsReceiptOptions(request);
        var sent0 = channel.request("acme.lending.v1.LoanQueryService/BrowseLoans");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "issued_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "attachments", "uri"),
                ProbeSupport.value(sent1, "purchase_requisition", "attachments", "uri"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "attachments", "file_name"),
                ProbeSupport.value(sent1, "purchase_requisition", "attachments", "file_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "attachments", "content_type"),
                ProbeSupport.value(sent1, "purchase_requisition", "attachments", "content_type"));
        var sent2 = channel.request("acme.procurement.v1.PurchaseOrderCalculationService/SimulatePurchaseOrder");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "purchase_requisition", "billing_address", "lines"),
                ProbeSupport.value(sent2, "details", "destination", "lines"));
        var sent3 = channel.request("acme.budgeting.v1.VarianceCalculationService/RecalculateVariances");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "purchase_requisition", "billing_address", "postal_code"),
                ProbeSupport.value(sent3, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "purchase_requisition", "audit", "created_at"),
                ProbeSupport.value(sent3, "variance", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.LoanQueryService/BrowseLoans"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseOrderCalculationService/SimulatePurchaseOrder"), "breakdown", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceCalculationService/RecalculateVariances"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.VarianceCalculationService/RecalculateVariances"), "variance", "audit", "updated_at"),
                ProbeSupport.value(response, "goods_receipt", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt_id"),
                ProbeSupport.value(response, "goods_receipt", "goods_receipt_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "goods_receipt", "number"),
                ProbeSupport.value(response, "goods_receipt", "counterparty", "number"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new GoodsReceiptCalculationApplicationEvents(AuditEventHistoryServiceGrpc.newStub(channel), BudgetLineCalculationServiceGrpc.newStub(channel), VarianceCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ListAuditEventVersionsResponse>();
        listener.on(new GoodsReceiptCalculationApplication.QuoteGoodsReceiptStage1Requested(ProbeSupport.sample(ListAuditEventVersionsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<RecalculateVariancesResponse>();
        listener.on(new GoodsReceiptCalculationApplication.CompareGoodsReceiptOptionsStage3Requested(ProbeSupport.sample(RecalculateVariancesRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        var completion2 = new CompletableFuture<QuoteBudgetLineResponse>();
        listener.on(new GoodsReceiptCalculationApplication.RecalculateGoodsReceiptsStage1Requested(ProbeSupport.sample(QuoteBudgetLineRequest.getDefaultInstance()), completion2, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion2.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion2.getNow(null));
    }
}

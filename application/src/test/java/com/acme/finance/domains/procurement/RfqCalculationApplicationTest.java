package com.acme.finance.domains.procurement;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.StreamRetentionPolicyExportRequest;
import com.acme.audit.v1.StreamRetentionPolicyExportResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.AnnotateJournalEntryVersionRequest;
import com.acme.ledger.v1.AnnotateJournalEntryVersionResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.lending.v1.ExplainLoanApplicationCalculationRequest;
import com.acme.lending.v1.ExplainLoanApplicationCalculationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.procurement.v1.CalculateRfqRequest;
import com.acme.procurement.v1.CalculateRfqResponse;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsRequest;
import com.acme.procurement.v1.CompareGoodsReceiptOptionsResponse;
import com.acme.procurement.v1.CompareRfqOptionsRequest;
import com.acme.procurement.v1.CompareRfqOptionsResponse;
import com.acme.procurement.v1.EstimateGoodsReceiptRequest;
import com.acme.procurement.v1.EstimateGoodsReceiptResponse;
import com.acme.procurement.v1.EstimateRfqRequest;
import com.acme.procurement.v1.EstimateRfqResponse;
import com.acme.procurement.v1.ExplainRfqCalculationRequest;
import com.acme.procurement.v1.ExplainRfqCalculationResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptCalculationServiceGrpc;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseOrderValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.QuoteRfqRequest;
import com.acme.procurement.v1.QuoteRfqResponse;
import com.acme.procurement.v1.RecalculateGoodsReceiptsRequest;
import com.acme.procurement.v1.RecalculateGoodsReceiptsResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.SimulateGoodsReceiptRequest;
import com.acme.procurement.v1.SimulateGoodsReceiptResponse;
import com.acme.procurement.v1.SimulatePurchaseOrderRequest;
import com.acme.procurement.v1.SimulatePurchaseOrderResponse;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.risk.v1.BrowseRiskScenariosRequest;
import com.acme.risk.v1.BrowseRiskScenariosResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RfqCalculationApplicationTest {
    @Test
    void calculateRfqPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var listener = new RfqCalculationApplicationEvents(JournalEntryHistoryServiceGrpc.newStub(channel), RiskScenarioQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RfqCalculationApplication.CalculateRfqStage2Requested command) {
                listener.on(command);
            }
            else if (event instanceof RfqCalculationApplication.SimulateRfqStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RfqCalculationApplication(
                FixedAssetServiceGrpc.newStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                BudgetLineValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                new RfqCalculationRequestMapperImpl(),
                GoodsReceiptCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CalculateRfqRequest.getDefaultInstance());
        var response = application.calculateRfq(request);
        var sent0 = channel.request("acme.procurement.v1.GoodsReceiptCalculationService/EstimateGoodsReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "lines"),
                ProbeSupport.value(sent0, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "audit", "created_by"),
                ProbeSupport.value(sent0, "goods_receipt", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent0, "goods_receipt", "billing_address", "region"));
        var sent1 = channel.request("acme.procurement.v1.GoodsReceiptCalculationService/SimulateGoodsReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "comments", "created_at"),
                ProbeSupport.value(sent1, "goods_receipt", "audit", "created_at"));
        var sent2 = channel.request("acme.risk.v1.RiskScenarioQueryService/CountRiskScenarios");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptCalculationService/EstimateGoodsReceipt"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "breakdown", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioQueryService/CountRiskScenarios"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq_id"),
                ProbeSupport.value(response, "rfq", "rfq_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "status"),
                ProbeSupport.value(response, "rfq", "status"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RfqCalculationApplicationEvents(JournalEntryHistoryServiceGrpc.newStub(channel), RiskScenarioQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CountRiskScenariosResponse>();
        listener.on(new RfqCalculationApplication.CalculateRfqStage2Requested(ProbeSupport.sample(CountRiskScenariosRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<AnnotateJournalEntryVersionResponse>();
        listener.on(new RfqCalculationApplication.SimulateRfqStage0Requested(ProbeSupport.sample(AnnotateJournalEntryVersionRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}

package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationCalculationServiceGrpc;
import com.acme.contracts.v1.ObligationComment;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.deliveries.v1.DeliveryServiceGrpc;
import com.acme.deliveries.v1.WatchDeliveriesRequest;
import com.acme.deliveries.v1.WatchDeliveriesResponse;
import com.acme.expenses.v1.CheckReceiptEligibilityRequest;
import com.acme.expenses.v1.CheckReceiptEligibilityResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptCardSettlement;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.procurement.v1.CountPurchaseOrdersRequest;
import com.acme.procurement.v1.CountPurchaseOrdersResponse;
import com.acme.procurement.v1.FindPurchaseOrdersRequest;
import com.acme.procurement.v1.FindPurchaseOrdersResponse;
import com.acme.procurement.v1.PurchaseOrderQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CalculateFxHedgeRequest;
import com.acme.treasury.v1.CalculateFxHedgeResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CheckBankTransferEligibilityRequest;
import com.acme.treasury.v1.CheckBankTransferEligibilityResponse;
import com.acme.treasury.v1.CloneCashPositionRequest;
import com.acme.treasury.v1.CloneCashPositionResponse;
import com.acme.treasury.v1.CreateCashPositionRequest;
import com.acme.treasury.v1.CreateCashPositionResponse;
import com.acme.treasury.v1.DeleteCashPositionRequest;
import com.acme.treasury.v1.DeleteCashPositionResponse;
import com.acme.treasury.v1.EstimateCashPositionRequest;
import com.acme.treasury.v1.EstimateCashPositionResponse;
import com.acme.treasury.v1.ExplainFxHedgeCalculationRequest;
import com.acme.treasury.v1.ExplainFxHedgeCalculationResponse;
import com.acme.treasury.v1.ExportCashPositionsRequest;
import com.acme.treasury.v1.ExportCashPositionsResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeBankSettlement;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeCardSettlement;
import com.acme.treasury.v1.GetCashPositionRequest;
import com.acme.treasury.v1.GetCashPositionResponse;
import com.acme.treasury.v1.GetFxHedgeRatesRequest;
import com.acme.treasury.v1.GetFxHedgeRatesResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.SearchCashPositionsRequest;
import com.acme.treasury.v1.SearchCashPositionsResponse;
import com.acme.treasury.v1.UpdateCashPositionRequest;
import com.acme.treasury.v1.UpdateCashPositionResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class CashPositionApplicationTest {
    @Test
    void deleteCashPositionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new CashPositionApplication(
                ChannelRegistry.fixed(channel),
                ObligationCalculationServiceGrpc.newBlockingStub(channel),
                DeliveryServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderQueryServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionQueryServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(DeleteCashPositionRequest.getDefaultInstance());
        var response = application.deleteCashPosition(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionQueryService/ExportCashPositions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "baggage"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.treasury.v1.CashPositionCalculationService/EstimateCashPosition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "etag"),
                ProbeSupport.value(sent1, "cash_position", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position_id"),
                ProbeSupport.value(sent1, "cash_position", "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent1, "cash_position", "contact", "email"));
        var sent2 = channel.request("acme.treasury.v1.FxHedgeCalculationService/CalculateFxHedge");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/EstimateCashPosition"), "cash_position", "card", "last4"),
                ProbeSupport.value(sent2, "fx_hedge", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/EstimateCashPosition"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionCalculationService/EstimateCashPosition"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/CalculateFxHedge"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
    }

    @Test
    void batchCreateCashPositionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new CashPositionApplication(
                ChannelRegistry.fixed(channel),
                ObligationCalculationServiceGrpc.newBlockingStub(channel),
                DeliveryServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newBlockingStub(channel),
                PurchaseOrderQueryServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferValidationServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionQueryServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(BatchCreateCashPositionsRequest.getDefaultInstance());
        var response = application.batchCreateCashPositions(request);
        var sent0 = channel.request("acme.treasury.v1.FxHedgeCalculationService/CalculateFxHedge");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_at"),
                ProbeSupport.value(sent0, "fx_hedge", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "updated_at"),
                ProbeSupport.value(sent0, "fx_hedge", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "card", "holder_name"),
                ProbeSupport.value(sent0, "fx_hedge", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeCalculationService/CalculateFxHedge"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        var endpoint = new CashPositionEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateCashPositionsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateCashPositionsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateCashPositions(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}

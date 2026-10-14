package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.ResolveAuditEventsRequest;
import com.acme.audit.v1.ResolveAuditEventsResponse;
import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsRequest;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowRequest;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubAttachment;
import com.acme.payroll.v1.PayStubWorkflowServiceGrpc;
import com.acme.payroll.v1.StartPayStubWorkflowRequest;
import com.acme.payroll.v1.StartPayStubWorkflowResponse;
import com.acme.scheduling.v1.BrowseShiftsRequest;
import com.acme.scheduling.v1.BrowseShiftsResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferBankSettlement;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CreateCashPositionRequest;
import com.acme.treasury.v1.CreateCashPositionResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.FxHedgeLineTax;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.GetCashPositionRequest;
import com.acme.treasury.v1.GetCashPositionResponse;
import com.acme.treasury.v1.GetFxHedgeSyncStateRequest;
import com.acme.treasury.v1.GetFxHedgeSyncStateResponse;
import com.acme.treasury.v1.LintCashPositionsRequest;
import com.acme.treasury.v1.LintCashPositionsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.PushFxHedgeRequest;
import com.acme.treasury.v1.PushFxHedgeResponse;
import com.acme.treasury.v1.RecalculateLiquidityForecastsRequest;
import com.acme.treasury.v1.RecalculateLiquidityForecastsResponse;
import com.acme.treasury.v1.ReconcileFxHedgesRequest;
import com.acme.treasury.v1.ReconcileFxHedgesResponse;
import com.acme.treasury.v1.ResolveFxHedgeConflictRequest;
import com.acme.treasury.v1.ResolveFxHedgeConflictResponse;
import com.acme.treasury.v1.SimulateCashPositionRequest;
import com.acme.treasury.v1.SimulateCashPositionResponse;
import com.acme.treasury.v1.StreamFxHedgeChangesRequest;
import com.acme.treasury.v1.StreamFxHedgeChangesResponse;
import com.acme.treasury.v1.SyncFxHedgesRequest;
import com.acme.treasury.v1.SyncFxHedgesResponse;
import com.acme.treasury.v1.ValidateFxHedgeRequest;
import com.acme.treasury.v1.ValidateFxHedgeResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class FxHedgeSyncApplicationTest {
    @Test
    void syncFxHedgesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new FxHedgeSyncApplication(
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimWorkflowServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                PayStubWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                ShiftQueryServiceGrpc.newBlockingStub(channel),
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SyncFxHedgesRequest.getDefaultInstance());
        var response = application.syncFxHedges(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionService/GetCashPosition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "counterparty", "cash_position_id"),
                ProbeSupport.value(sent0, "cash_position_id"));
        var sent1 = channel.request("acme.payroll.v1.PayStubWorkflowService/StartPayStubWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "attachments", "checksum"),
                ProbeSupport.value(sent1, "pay_stub", "attachments", "checksum"));
        var sent2 = channel.request("acme.treasury.v1.CashPositionValidationService/LintCashPositions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionService/GetCashPosition"), "cash_position", "card", "network"),
                ProbeSupport.value(sent2, "cash_position", "card", "network"));
        var sent3 = channel.request("acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "billing_address", "lines"),
                ProbeSupport.value(sent3, "details", "destination", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "billing_address", "region"),
                ProbeSupport.value(sent3, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionService/GetCashPosition"), "cash_position", "cash_position_id"),
                ProbeSupport.value(response, "fx_hedge", "counterparty", "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionService/GetCashPosition"), "cash_position", "holder", "number"),
                ProbeSupport.value(response, "fx_hedge", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/LintCashPositions"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/LintCashPositions"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordCalculationService/RecalculateConsentRecords"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "fx_hedge", "counterparty", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge_id"),
                ProbeSupport.value(response, "fx_hedge", "fx_hedge_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "notes"),
                ProbeSupport.value(response, "fx_hedge", "notes"));
    }

    @Test
    void streamFxHedgeChangesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new FxHedgeSyncApplication(
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                MileageClaimWorkflowServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                PayStubWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                ShiftQueryServiceGrpc.newBlockingStub(channel),
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newBlockingStub(channel),
                LiquidityForecastCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamFxHedgeChangesRequest.getDefaultInstance());
        var response = application.streamFxHedgeChanges(request);
        var sent0 = channel.request("acme.scheduling.v1.ShiftQueryService/BrowseShifts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "counterparty", "cash_position_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.treasury.v1.BankTransferCalculationService/ExplainBankTransferCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "bank", "iban"),
                ProbeSupport.value(sent1, "bank_transfer", "bank", "iban"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "billing_address", "region"),
                ProbeSupport.value(sent1, "bank_transfer", "billing_address", "region"));
        var sent2 = channel.request("acme.treasury.v1.LiquidityForecastCalculationService/RecalculateLiquidityForecasts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "billing_address", "recipient"),
                ProbeSupport.value(sent2, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "created_by"),
                ProbeSupport.value(sent2, "liquidity_forecast", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.scheduling.v1.ShiftQueryService/BrowseShifts"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/ExplainBankTransferCalculation"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/ExplainBankTransferCalculation"), "bank_transfer", "billing_address", "country_code"),
                ProbeSupport.value(response, "fx_hedge", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastCalculationService/RecalculateLiquidityForecasts"), "liquidity_forecast", "metadata"),
                ProbeSupport.value(response, "fx_hedge", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.LiquidityForecastCalculationService/RecalculateLiquidityForecasts"), "liquidity_forecast", "audit", "updated_at"),
                ProbeSupport.value(response, "fx_hedge", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "ack", "sequence"),
                ProbeSupport.value(response, "sequence"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "fx_hedge_id"),
                ProbeSupport.value(response, "fx_hedge", "fx_hedge_id"));
        var endpoint = new FxHedgeSyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamFxHedgeChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamFxHedgeChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamFxHedgeChanges(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }
}

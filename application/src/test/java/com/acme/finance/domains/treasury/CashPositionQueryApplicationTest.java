package com.acme.finance.domains.treasury;

import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.ValidateLoanRequest;
import com.acme.lending.v1.ValidateLoanResponse;
import com.acme.patients.v1.PatientRef;
import com.acme.prescriptions.v1.Prescription;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.PrescriptionLine;
import com.acme.prescriptions.v1.QuotePrescriptionRequest;
import com.acme.prescriptions.v1.QuotePrescriptionResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.BrowseCashPositionsRequest;
import com.acme.treasury.v1.BrowseCashPositionsResponse;
import com.acme.treasury.v1.CalculateCashPositionRequest;
import com.acme.treasury.v1.CalculateCashPositionResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CheckFxHedgeEligibilityRequest;
import com.acme.treasury.v1.CheckFxHedgeEligibilityResponse;
import com.acme.treasury.v1.CountCashPositionsRequest;
import com.acme.treasury.v1.CountCashPositionsResponse;
import com.acme.treasury.v1.ExportCashPositionsRequest;
import com.acme.treasury.v1.ExportCashPositionsResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.LintFxHedgesRequest;
import com.acme.treasury.v1.LintFxHedgesResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LookupCashPositionRequest;
import com.acme.treasury.v1.LookupCashPositionResponse;
import com.acme.treasury.v1.PeekCashPositionRequest;
import com.acme.treasury.v1.PeekCashPositionResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.ResolveCashPositionsRequest;
import com.acme.treasury.v1.ResolveCashPositionsResponse;
import com.acme.treasury.v1.SuggestCashPositionsRequest;
import com.acme.treasury.v1.SuggestCashPositionsResponse;
import com.acme.treasury.v1.SummarizeCashPositionsRequest;
import com.acme.treasury.v1.SummarizeCashPositionsResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class CashPositionQueryApplicationTest {
    @Test
    void exportCashPositionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new CashPositionQueryApplication(
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newFutureStub(channel),
                PrescriptionCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(ExportCashPositionsRequest.getDefaultInstance());
        var response = application.exportCashPositions(request);
        var sent0 = channel.request("acme.treasury.v1.FxHedgeValidationService/LintFxHedges");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "fx_hedge", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.treasury.v1.FxHedgeValidationService/CheckFxHedgeEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeValidationService/LintFxHedges"), "fx_hedge", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "fx_hedge", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeValidationService/LintFxHedges"), "fx_hedge", "audit", "etag"),
                ProbeSupport.value(sent1, "fx_hedge", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeValidationService/LintFxHedges"), "fx_hedge", "audit", "updated_at"),
                ProbeSupport.value(response, "cash_position", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.FxHedgeValidationService/CheckFxHedgeEligibility"), "fx_hedge", "audit", "updated_by"),
                ProbeSupport.value(response, "cash_position", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(response, "cash_position", "labels"));
        var endpoint = new CashPositionQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportCashPositionsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportCashPositionsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportCashPositions(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeCashPositionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new CashPositionQueryApplication(
                StandingOrderValidationServiceGrpc.newBlockingStub(channel),
                BudgetLineCalculationServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newFutureStub(channel),
                PrescriptionCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeValidationServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(SummarizeCashPositionsRequest.getDefaultInstance());
        var response = application.summarizeCashPositions(request);
        var sent0 = channel.request("acme.banking.v1.StandingOrderValidationService/ExplainStandingOrderDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "standing_order", "organizer", "email"));
        var sent1 = channel.request("acme.treasury.v1.CashPositionCalculationService/CalculateCashPosition");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ExplainStandingOrderDecision"), "standing_order", "location", "country_code"),
                ProbeSupport.value(sent1, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ExplainStandingOrderDecision"), "standing_order", "audit", "etag"),
                ProbeSupport.value(sent1, "cash_position", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ExplainStandingOrderDecision"), "standing_order", "location", "region"),
                ProbeSupport.value(sent1, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ExplainStandingOrderDecision"), "standing_order", "location", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderValidationService/ExplainStandingOrderDecision"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}

package com.acme.finance.domains.treasury;

import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.ListAssetTransferConflictsRequest;
import com.acme.assets.v1.ListAssetTransferConflictsResponse;
import com.acme.banking.v1.DelegateMandateApprovalRequest;
import com.acme.banking.v1.DelegateMandateApprovalResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateBankSettlement;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalRequest;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyApprovalServiceGrpc;
import com.acme.expenses.v1.RecallExpensePolicyRequest;
import com.acme.expenses.v1.RecallExpensePolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.giftcards.v1.GiftCard;
import com.acme.giftcards.v1.GiftCardCalculationServiceGrpc;
import com.acme.giftcards.v1.GiftCardLimit;
import com.acme.giftcards.v1.SimulateGiftCardRequest;
import com.acme.giftcards.v1.SimulateGiftCardResponse;
import com.acme.identity.v1.UserRef;
import com.acme.travel.v1.CompareTripOptionsRequest;
import com.acme.travel.v1.CompareTripOptionsResponse;
import com.acme.travel.v1.Trip;
import com.acme.travel.v1.TripCalculationServiceGrpc;
import com.acme.travel.v1.TripCardSettlement;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowRequest;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionApprovalServiceGrpc;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
import com.acme.treasury.v1.DelegateCashPositionApprovalRequest;
import com.acme.treasury.v1.DelegateCashPositionApprovalResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GetCashPositionLifecycleRequest;
import com.acme.treasury.v1.GetCashPositionLifecycleResponse;
import com.acme.treasury.v1.InspectCashPositionWorkflowRequest;
import com.acme.treasury.v1.InspectCashPositionWorkflowResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.ListCashPositionWorkflowsRequest;
import com.acme.treasury.v1.ListCashPositionWorkflowsResponse;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.ResumeCashPositionWorkflowRequest;
import com.acme.treasury.v1.ResumeCashPositionWorkflowResponse;
import com.acme.treasury.v1.RetryCashPositionWorkflowRequest;
import com.acme.treasury.v1.RetryCashPositionWorkflowResponse;
import com.acme.treasury.v1.SignalCashPositionWorkflowRequest;
import com.acme.treasury.v1.SignalCashPositionWorkflowResponse;
import com.acme.treasury.v1.StartCashPositionWorkflowRequest;
import com.acme.treasury.v1.StartCashPositionWorkflowResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class CashPositionWorkflowApplicationTest {
    @Test
    void resumeCashPositionWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var listener = new CashPositionWorkflowApplicationEvents(HouseholdCalculationServiceGrpc.newStub(channel), CashPositionLifecycleServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof CashPositionWorkflowApplication.ResumeCashPositionWorkflowStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof CashPositionWorkflowApplication.SignalCashPositionWorkflowStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new CashPositionWorkflowApplication(
                MandateApprovalServiceGrpc.newBlockingStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationReportingServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                ConsentRecordValidationServiceGrpc.newFutureStub(channel),
                events,
                ExpensePolicyApprovalServiceGrpc.newBlockingStub(channel),
                GiftCardCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                TripCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionApprovalServiceGrpc.newBlockingStub(channel),
                FxHedgeSyncServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(ResumeCashPositionWorkflowRequest.getDefaultInstance());
        var response = application.resumeCashPositionWorkflow(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "cash_position_id"),
                ProbeSupport.value(sent0, "cash_position_id"));
        var sent1 = channel.request("acme.treasury.v1.CashPositionApprovalService/DelegateCashPositionApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "cash_position", "contact", "preferred_channel"),
                ProbeSupport.value(sent1, "cash_position", "contact", "preferred_channel"));
        var sent2 = channel.request("acme.travel.v1.TripCalculationService/CompareTripOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "card", "network"),
                ProbeSupport.value(sent2, "trip", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionApprovalService/DelegateCashPositionApproval"), "cash_position", "audit", "created_by"),
                ProbeSupport.value(sent2, "trip", "audit", "created_by"));
        var sent3 = channel.request("acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "holder", "display_name"),
                ProbeSupport.value(sent3, "consent_record", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "cash_position", "contact", "name"),
                ProbeSupport.value(sent3, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionLifecycleService/GetCashPositionLifecycle"), "cash_position", "audit", "created_by"),
                ProbeSupport.value(response, "cash_position", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionApprovalService/DelegateCashPositionApproval"), "cash_position", "audit", "created_at"),
                ProbeSupport.value(response, "cash_position", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.travel.v1.TripCalculationService/CompareTripOptions"), "breakdown", "currency_code"),
                ProbeSupport.value(response, "cash_position", "currency_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.travel.v1.TripCalculationService/CompareTripOptions"), "trip", "audit", "updated_at"),
                ProbeSupport.value(response, "cash_position", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.ConsentRecordValidationService/NormalizeConsentRecord"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position_id"),
                ProbeSupport.value(response, "cash_position", "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "holder"),
                ProbeSupport.value(response, "cash_position", "holder"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new CashPositionWorkflowApplicationEvents(HouseholdCalculationServiceGrpc.newStub(channel), CashPositionLifecycleServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<GetCashPositionLifecycleResponse>();
        listener.on(new CashPositionWorkflowApplication.ResumeCashPositionWorkflowStage0Requested(ProbeSupport.sample(GetCashPositionLifecycleRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<CompareHouseholdOptionsResponse>();
        listener.on(new CashPositionWorkflowApplication.SignalCashPositionWorkflowStage1Requested(ProbeSupport.sample(CompareHouseholdOptionsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}

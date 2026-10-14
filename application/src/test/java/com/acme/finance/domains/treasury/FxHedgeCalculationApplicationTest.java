package com.acme.finance.domains.treasury;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BatchCreateBudgetsRequest;
import com.acme.budgeting.v1.BatchCreateBudgetsResponse;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.recommendations.v1.CloneRecommendationSetRequest;
import com.acme.recommendations.v1.CloneRecommendationSetResponse;
import com.acme.recommendations.v1.RecommendationSet;
import com.acme.recommendations.v1.RecommendationSetCardSettlement;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferBankSettlement;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.CalculateBankTransferRequest;
import com.acme.treasury.v1.CalculateBankTransferResponse;
import com.acme.treasury.v1.CalculateFxHedgeRequest;
import com.acme.treasury.v1.CalculateFxHedgeResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CompareFxHedgeOptionsRequest;
import com.acme.treasury.v1.CompareFxHedgeOptionsResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.ExplainFxHedgeCalculationRequest;
import com.acme.treasury.v1.ExplainFxHedgeCalculationResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetFxHedgeRatesRequest;
import com.acme.treasury.v1.GetFxHedgeRatesResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.ScoreCashPositionRequest;
import com.acme.treasury.v1.ScoreCashPositionResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class FxHedgeCalculationApplicationTest {
    @Test
    void compareFxHedgeOptionsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new FxHedgeCalculationApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                BudgetServiceGrpc.newStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionQueryServiceGrpc.newBlockingStub(channel),
                RecommendationSetServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CompareFxHedgeOptionsRequest.getDefaultInstance());
        var response = application.compareFxHedgeOptions(request);
        var sent0 = channel.request("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "billing_address", "recipient"),
                ProbeSupport.value(sent0, "details", "destination", "recipient"));
        var sent1 = channel.request("acme.recommendations.v1.RecommendationSetService/CloneRecommendationSet");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "card", "network"),
                ProbeSupport.value(sent1, "recommendation_set", "card", "network"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "card", "holder_name"),
                ProbeSupport.value(sent1, "recommendation_set", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers"), "bank_transfer", "counterparty", "email"),
                ProbeSupport.value(sent1, "recommendation_set", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers"), "bank_transfer", "audit", "updated_at"),
                ProbeSupport.value(sent1, "recommendation_set", "audit", "updated_at"));
        var sent2 = channel.request("acme.treasury.v1.BankTransferCalculationService/CalculateBankTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.recommendations.v1.RecommendationSetService/CloneRecommendationSet"), "recommendation_set", "audit", "created_at"),
                ProbeSupport.value(sent2, "bank_transfer", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "billing_address", "locality"),
                ProbeSupport.value(sent2, "bank_transfer", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "billing_address", "region"),
                ProbeSupport.value(sent2, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers"), "bank_transfer", "card", "holder_name"),
                ProbeSupport.value(sent2, "bank_transfer", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/RecalculateBankTransfers"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.recommendations.v1.RecommendationSetService/CloneRecommendationSet"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/CalculateBankTransfer"), "bank_transfer", "billing_address", "locality"),
                ProbeSupport.value(response, "fx_hedge", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/CalculateBankTransfer"), "bank_transfer", "number"),
                ProbeSupport.value(response, "fx_hedge", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge_id"),
                ProbeSupport.value(response, "fx_hedge", "fx_hedge_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fx_hedge", "notes"),
                ProbeSupport.value(response, "fx_hedge", "notes"));
    }
}

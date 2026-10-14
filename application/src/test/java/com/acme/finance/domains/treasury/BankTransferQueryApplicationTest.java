package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.fuel.v1.EstimateFuelCardRequest;
import com.acme.fuel.v1.EstimateFuelCardResponse;
import com.acme.fuel.v1.FuelCard;
import com.acme.fuel.v1.FuelCardCalculationServiceGrpc;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BrowseBankTransfersRequest;
import com.acme.treasury.v1.BrowseBankTransfersResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.CountBankTransfersRequest;
import com.acme.treasury.v1.CountBankTransfersResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.FindBankTransfersRequest;
import com.acme.treasury.v1.FindBankTransfersResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LookupBankTransferRequest;
import com.acme.treasury.v1.LookupBankTransferResponse;
import com.acme.treasury.v1.PeekBankTransferRequest;
import com.acme.treasury.v1.PeekBankTransferResponse;
import com.acme.treasury.v1.ResolveBankTransfersRequest;
import com.acme.treasury.v1.ResolveBankTransfersResponse;
import com.acme.treasury.v1.SimulateFxHedgeRequest;
import com.acme.treasury.v1.SimulateFxHedgeResponse;
import com.acme.treasury.v1.SuggestBankTransfersRequest;
import com.acme.treasury.v1.SuggestBankTransfersResponse;
import com.acme.treasury.v1.ValidateCashPositionRequest;
import com.acme.treasury.v1.ValidateCashPositionResponse;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class BankTransferQueryApplicationTest {
    @Test
    void resolveBankTransfersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new BankTransferQueryApplication(
                FuelCardCalculationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                repository,
                BankTransferCalculationServiceGrpc.newBlockingStub(channel),
                CashPositionValidationServiceGrpc.newBlockingStub(channel),
                FxHedgeCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ResolveBankTransfersRequest.getDefaultInstance());
        var response = application.resolveBankTransfers(request);
        var sent0 = channel.request("acme.treasury.v1.CashPositionValidationService/CheckCashPositionEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "cash_position", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent0, "cash_position", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "cash_position", "external_account_id"));
        var sent1 = channel.request("acme.treasury.v1.BankTransferCalculationService/ExplainBankTransferCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/CheckCashPositionEligibility"), "cash_position", "contact", "phone"),
                ProbeSupport.value(sent1, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.CashPositionValidationService/CheckCashPositionEligibility"), "cash_position", "holder", "number"),
                ProbeSupport.value(response, "bank_transfers", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/ExplainBankTransferCalculation"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.treasury.v1.BankTransferCalculationService/ExplainBankTransferCalculation"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
    }
}

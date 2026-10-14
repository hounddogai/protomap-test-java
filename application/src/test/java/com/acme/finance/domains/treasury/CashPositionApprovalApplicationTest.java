package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationApprovalServiceGrpc;
import com.acme.compliance.v1.CommentAttestationApprovalRequest;
import com.acme.compliance.v1.CommentAttestationApprovalResponse;
import com.acme.contracts.v1.AdvanceContractWorkflowRequest;
import com.acme.contracts.v1.AdvanceContractWorkflowResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractCardSettlement;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.RenewalRef;
import com.acme.customers.v1.CalculateConsentRecordRequest;
import com.acme.customers.v1.CalculateConsentRecordResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CountCustomersRequest;
import com.acme.customers.v1.CountCustomersResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.PreviewConsentRecordRequest;
import com.acme.customers.v1.PreviewConsentRecordResponse;
import com.acme.customers.v1.PreviewCustomerRequest;
import com.acme.customers.v1.PreviewCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.insurance.v1.ListCoverageReportsRequest;
import com.acme.insurance.v1.ListCoverageReportsResponse;
import com.acme.returns.v1.CreateReturnRequestRequest;
import com.acme.returns.v1.CreateReturnRequestResponse;
import com.acme.returns.v1.ReturnRequest;
import com.acme.returns.v1.ReturnRequestCardSettlement;
import com.acme.returns.v1.ReturnRequestServiceGrpc;
import com.acme.treasury.v1.ApproveCashPositionRequest;
import com.acme.treasury.v1.ApproveCashPositionResponse;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferReportingServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CommentCashPositionApprovalRequest;
import com.acme.treasury.v1.CommentCashPositionApprovalResponse;
import com.acme.treasury.v1.CountFxHedgeTotalsRequest;
import com.acme.treasury.v1.CountFxHedgeTotalsResponse;
import com.acme.treasury.v1.DelegateCashPositionApprovalRequest;
import com.acme.treasury.v1.DelegateCashPositionApprovalResponse;
import com.acme.treasury.v1.DeleteCashPositionRequest;
import com.acme.treasury.v1.DeleteCashPositionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.FxHedgeReportingServiceGrpc;
import com.acme.treasury.v1.GetCashPositionApprovalStatusRequest;
import com.acme.treasury.v1.GetCashPositionApprovalStatusResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastBankSettlement;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.ListPendingCashPositionsRequest;
import com.acme.treasury.v1.ListPendingCashPositionsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.RecallCashPositionRequest;
import com.acme.treasury.v1.RecallCashPositionResponse;
import com.acme.treasury.v1.ScheduleBankTransferReportRequest;
import com.acme.treasury.v1.ScheduleBankTransferReportResponse;
import com.acme.treasury.v1.SubmitCashPositionRequest;
import com.acme.treasury.v1.SubmitCashPositionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class CashPositionApprovalApplicationTest {
    @Test
    void commentCashPositionApprovalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new TreasuryRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveBankTransfer(String tenant, BankTransfer value) {}
            public void saveCashPosition(String tenant, CashPosition value) {}
            public void saveFxHedge(String tenant, FxHedge value) {}
            public void saveLiquidityForecast(String tenant, LiquidityForecast value) {}
        };
        var application = new CashPositionApprovalApplication(
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationApprovalServiceGrpc.newBlockingStub(channel),
                ContractWorkflowServiceGrpc.newBlockingStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                ConsentRecordValidationServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                new CashPositionApprovalRequestMapperImpl(),
                repository,
                ReturnRequestServiceGrpc.newBlockingStub(channel),
                BankTransferReportingServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newBlockingStub(channel),
                FxHedgeReportingServiceGrpc.newBlockingStub(channel),
                LiquidityForecastImportServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(CommentCashPositionApprovalRequest.getDefaultInstance());
        var response = application.commentCashPositionApproval(request);
        var sent0 = channel.request("acme.returns.v1.ReturnRequestService/CreateReturnRequest");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "card", "last4"),
                ProbeSupport.value(sent0, "return_request", "card", "last4"));
        var sent1 = channel.request("acme.customers.v1.CustomerQueryService/CountCustomers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "labels"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.returns.v1.ReturnRequestService/CreateReturnRequest"), "return_request", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "contact", "phone"),
                ProbeSupport.value(sent2, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.returns.v1.ReturnRequestService/CreateReturnRequest"), "return_request", "counterparty", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "contact", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        var sent3 = channel.request("acme.insurance.v1.CoverageReportingService/ListCoverageReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.returns.v1.ReturnRequestService/CreateReturnRequest"), "return_request", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position", "labels"),
                ProbeSupport.value(sent3, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.returns.v1.ReturnRequestService/CreateReturnRequest"), "return_request", "audit", "updated_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "retention_policy", "owner", "user_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.returns.v1.ReturnRequestService/CreateReturnRequest"), "return_request", "number"),
                ProbeSupport.value(response, "cash_position", "holder", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.returns.v1.ReturnRequestService/CreateReturnRequest"), "return_request", "counterparty", "display_name"),
                ProbeSupport.value(response, "cash_position", "holder", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerQueryService/CountCustomers"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyNotificationService/SubscribeRetentionPolicyUpdates"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageReportingService/ListCoverageReports"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "cash_position_id"),
                ProbeSupport.value(response, "cash_position", "cash_position_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}

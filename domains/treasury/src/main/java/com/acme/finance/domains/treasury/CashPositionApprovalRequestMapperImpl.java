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
import com.acme.insurance.v1.ListCoverageReportsRequest;
import com.acme.insurance.v1.ListCoverageReportsResponse;
import com.acme.returns.v1.CreateReturnRequestRequest;
import com.acme.returns.v1.CreateReturnRequestResponse;
import com.acme.returns.v1.ReturnRequest;
import com.acme.returns.v1.ReturnRequestCardSettlement;
import com.acme.returns.v1.ReturnRequestServiceGrpc;
import com.acme.treasury.v1.ApproveCashPositionRequest;
import com.acme.treasury.v1.ApproveCashPositionResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class CashPositionApprovalRequestMapperImpl implements CashPositionApprovalRequestMapper {
    @Override
    public PreviewConsentRecordRequest recallCashPositionStage2(RecallCashPositionRequest request, PreviewCustomerResponse reply0) {
        return PreviewConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(reply0.getCustomer().getRelationshipManager().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PreviewConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getCustomer().getName())
                                .build())
                        .build())
                .build();
    }

    @Override
    public UnsubscribeAuditTrailUpdatesRequest getCashPositionApprovalStatusStage0(GetCashPositionApprovalStatusRequest request) {
        return UnsubscribeAuditTrailUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeAuditTrailUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllVariables(request.getContext().getBaggageMap())
                        .build())
                .build();
    }

    @Override
    public CountCustomersRequest commentCashPositionApprovalStage1(CommentCashPositionApprovalRequest request, CreateReturnRequestResponse reply0) {
        return CountCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCustomersRequest.Filter.newBuilder()
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .setRegion(reply0.getReturnRequest().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}

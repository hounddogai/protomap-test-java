package com.acme.finance.domains.procurement;

import com.acme.appointments.v1.AppointmentQueryServiceGrpc;
import com.acme.appointments.v1.CountAppointmentsRequest;
import com.acme.appointments.v1.CountAppointmentsResponse;
import com.acme.assets.v1.AssetTransferWorkflowServiceGrpc;
import com.acme.assets.v1.ListAssetTransferWorkflowsRequest;
import com.acme.assets.v1.ListAssetTransferWorkflowsResponse;
import com.acme.audit.v1.AnnotateAuditTrailVersionRequest;
import com.acme.audit.v1.AnnotateAuditTrailVersionResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.RecordAuditTrailVersionRequest;
import com.acme.audit.v1.RecordAuditTrailVersionResponse;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.UnsubscribeAuditTrailUpdatesResponse;
import com.acme.budgeting.v1.CountForecastTotalsRequest;
import com.acme.budgeting.v1.CountForecastTotalsResponse;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.RenewalParticipant;
import com.acme.contracts.v1.RenewalWorkflowServiceGrpc;
import com.acme.contracts.v1.RetryContractWorkflowRequest;
import com.acme.contracts.v1.RetryContractWorkflowResponse;
import com.acme.contracts.v1.StartRenewalWorkflowRequest;
import com.acme.contracts.v1.StartRenewalWorkflowResponse;
import com.acme.customers.v1.CalculateCustomerSegmentRequest;
import com.acme.customers.v1.CalculateCustomerSegmentResponse;
import com.acme.customers.v1.CloneCustomerRequest;
import com.acme.customers.v1.CloneCustomerResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.giftcards.v1.CountGiftCardsRequest;
import com.acme.giftcards.v1.CountGiftCardsResponse;
import com.acme.giftcards.v1.GiftCardQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanLimit;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.procurement.v1.ApproveGoodsReceiptRequest;
import com.acme.procurement.v1.ApproveGoodsReceiptResponse;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalRequest;
import com.acme.procurement.v1.CommentGoodsReceiptApprovalResponse;
import com.acme.procurement.v1.DelegateGoodsReceiptApprovalRequest;
import com.acme.procurement.v1.DelegateGoodsReceiptApprovalResponse;
import com.acme.procurement.v1.EscalateGoodsReceiptRequest;
import com.acme.procurement.v1.EscalateGoodsReceiptResponse;
import com.acme.procurement.v1.GetGoodsReceiptApprovalStatusRequest;
import com.acme.procurement.v1.GetGoodsReceiptApprovalStatusResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLifecycleServiceGrpc;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.ListPendingGoodsReceiptsRequest;
import com.acme.procurement.v1.ListPendingGoodsReceiptsResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.procurement.v1.RecallGoodsReceiptRequest;
import com.acme.procurement.v1.RecallGoodsReceiptResponse;
import com.acme.procurement.v1.ReinstateGoodsReceiptRequest;
import com.acme.procurement.v1.ReinstateGoodsReceiptResponse;
import com.acme.procurement.v1.RejectGoodsReceiptRequest;
import com.acme.procurement.v1.RejectGoodsReceiptResponse;
import com.acme.procurement.v1.SubmitGoodsReceiptRequest;
import com.acme.procurement.v1.SubmitGoodsReceiptResponse;
import com.acme.procurement.v1.UpdatePurchaseRequisitionRequest;
import com.acme.procurement.v1.UpdatePurchaseRequisitionResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class GoodsReceiptApprovalRequestMapperImpl implements GoodsReceiptApprovalRequestMapper {
    @Override
    public RetryContractWorkflowRequest recallGoodsReceiptStage1(RecallGoodsReceiptRequest request) {
        return RetryContractWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getGoodsReceipt().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
    }
}

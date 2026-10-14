package com.acme.finance.domains.procurement;

import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.CloneAccessPolicyRequest;
import com.acme.authorization.v1.CloneAccessPolicyResponse;
import com.acme.budgeting.v1.BudgetLineRef;
import com.acme.budgeting.v1.EscalateVarianceRequest;
import com.acme.budgeting.v1.EscalateVarianceResponse;
import com.acme.budgeting.v1.SubmitVarianceRequest;
import com.acme.budgeting.v1.SubmitVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceApprovalServiceGrpc;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationBankSettlement;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseRef;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlBlockAction;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.compliance.v1.ListAttestationWorkflowsRequest;
import com.acme.compliance.v1.ListAttestationWorkflowsResponse;
import com.acme.compliance.v1.PauseAttestationWorkflowRequest;
import com.acme.compliance.v1.PauseAttestationWorkflowResponse;
import com.acme.compliance.v1.StartControlWorkflowRequest;
import com.acme.compliance.v1.StartControlWorkflowResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.RecalculatePreferencesRequest;
import com.acme.customers.v1.RecalculatePreferencesResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.SchedulePolicyExpiryRequest;
import com.acme.insurance.v1.SchedulePolicyExpiryResponse;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionRequest;
import com.acme.ledger.v1.AnnotateLedgerAccountVersionResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.procurement.v1.ActivatePurchaseOrderRequest;
import com.acme.procurement.v1.ActivatePurchaseOrderResponse;
import com.acme.procurement.v1.CalculatePurchaseRequisitionRequest;
import com.acme.procurement.v1.CalculatePurchaseRequisitionResponse;
import com.acme.procurement.v1.ExpirePurchaseOrderRequest;
import com.acme.procurement.v1.ExpirePurchaseOrderResponse;
import com.acme.procurement.v1.ExportPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ExportPurchaseRequisitionsResponse;
import com.acme.procurement.v1.FindPurchaseOrdersRequest;
import com.acme.procurement.v1.FindPurchaseOrdersResponse;
import com.acme.procurement.v1.GetPurchaseOrderLifecycleRequest;
import com.acme.procurement.v1.GetPurchaseOrderLifecycleResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptLine;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.ListPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PreviewPurchaseOrderRequest;
import com.acme.procurement.v1.PreviewPurchaseOrderResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.ReinstatePurchaseOrderRequest;
import com.acme.procurement.v1.ReinstatePurchaseOrderResponse;
import com.acme.procurement.v1.RenewPurchaseOrderRequest;
import com.acme.procurement.v1.RenewPurchaseOrderResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryRequest;
import com.acme.procurement.v1.SchedulePurchaseOrderExpiryResponse;
import com.acme.procurement.v1.ScoreGoodsReceiptRequest;
import com.acme.procurement.v1.ScoreGoodsReceiptResponse;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsRequest;
import com.acme.procurement.v1.SummarizePurchaseRequisitionsResponse;
import com.acme.procurement.v1.SuspendPurchaseOrderRequest;
import com.acme.procurement.v1.SuspendPurchaseOrderResponse;
import com.acme.procurement.v1.TerminatePurchaseOrderRequest;
import com.acme.procurement.v1.TerminatePurchaseOrderResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class PurchaseOrderLifecycleRequestMapperImpl implements PurchaseOrderLifecycleRequestMapper {
    @Override
    public FindPurchaseOrdersRequest expirePurchaseOrderStage1(ExpirePurchaseOrderRequest request, SchedulePolicyExpiryResponse reply0) {
        return FindPurchaseOrdersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindPurchaseOrdersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPolicy().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getPolicy().getLabelsMap())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }
}

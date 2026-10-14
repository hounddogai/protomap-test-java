package com.acme.finance.domains.expenses;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowRequest;
import com.acme.collections.v1.AdvanceCollectionCaseWorkflowResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.AdvanceReceiptWorkflowRequest;
import com.acme.expenses.v1.AdvanceReceiptWorkflowResponse;
import com.acme.expenses.v1.ApproveExpenseReportRequest;
import com.acme.expenses.v1.ApproveExpenseReportResponse;
import com.acme.expenses.v1.CommentExpenseReportApprovalRequest;
import com.acme.expenses.v1.CommentExpenseReportApprovalResponse;
import com.acme.expenses.v1.CountExpensePolicyTotalsRequest;
import com.acme.expenses.v1.CountExpensePolicyTotalsResponse;
import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.ExpensePolicyReportingServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportApprovalServiceGrpc;
import com.acme.expenses.v1.ExpenseReportLine;
import com.acme.expenses.v1.ExpenseReportLineTax;
import com.acme.expenses.v1.ExpireReceiptRequest;
import com.acme.expenses.v1.ExpireReceiptResponse;
import com.acme.expenses.v1.GenerateExpensePolicyReportRequest;
import com.acme.expenses.v1.GenerateExpensePolicyReportResponse;
import com.acme.expenses.v1.InspectReceiptWorkflowRequest;
import com.acme.expenses.v1.InspectReceiptWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.MileageClaimReportingServiceGrpc;
import com.acme.expenses.v1.MileageClaimValidationServiceGrpc;
import com.acme.expenses.v1.PauseReceiptWorkflowRequest;
import com.acme.expenses.v1.PauseReceiptWorkflowResponse;
import com.acme.expenses.v1.PreviewMileageClaimRequest;
import com.acme.expenses.v1.PreviewMileageClaimResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.RetryReceiptWorkflowRequest;
import com.acme.expenses.v1.RetryReceiptWorkflowResponse;
import com.acme.expenses.v1.SignalReceiptWorkflowRequest;
import com.acme.expenses.v1.SignalReceiptWorkflowResponse;
import com.acme.expenses.v1.StartReceiptWorkflowRequest;
import com.acme.expenses.v1.StartReceiptWorkflowResponse;
import com.acme.expenses.v1.TerminateReceiptRequest;
import com.acme.expenses.v1.TerminateReceiptResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.CompareJournalEntryVersionsRequest;
import com.acme.ledger.v1.CompareJournalEntryVersionsResponse;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryAttachment;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class ReceiptWorkflowRequestMapperImpl implements ReceiptWorkflowRequestMapper {
    @Override
    public PullAssetTransfersRequest signalReceiptWorkflowStage0(SignalReceiptWorkflowRequest request) {
        return PullAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getReceipt().getAudit().getCreatedAt())
                                .setCreatedBy(request.getReceipt().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getReceipt().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PullAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
    }
}

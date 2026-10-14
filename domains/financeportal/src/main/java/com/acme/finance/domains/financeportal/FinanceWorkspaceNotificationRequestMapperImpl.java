package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AnnotateRetentionPolicyVersionRequest;
import com.acme.audit.v1.AnnotateRetentionPolicyVersionResponse;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.ListAuditTrailVersionsRequest;
import com.acme.audit.v1.ListAuditTrailVersionsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.billing.v1.CreditNoteWorkflowServiceGrpc;
import com.acme.billing.v1.RetryCreditNoteWorkflowRequest;
import com.acme.billing.v1.RetryCreditNoteWorkflowResponse;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.customers.v1.ApproveHouseholdRequest;
import com.acme.customers.v1.ApproveHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdApprovalServiceGrpc;
import com.acme.edgegateway.v1.CloneRouteBindingRequest;
import com.acme.edgegateway.v1.CloneRouteBindingResponse;
import com.acme.edgegateway.v1.RouteBinding;
import com.acme.edgegateway.v1.RouteBindingServiceGrpc;
import com.acme.fanportal.v1.CountFanSessionsRequest;
import com.acme.fanportal.v1.CountFanSessionsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventRequest;
import com.acme.financeportal.v1.AcknowledgeFinanceWorkspaceEventResponse;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspace;
import com.acme.financeportal.v1.FinanceWorkspaceIntegrationServiceGrpc;
import com.acme.financeportal.v1.FinanceWorkspaceSla;
import com.acme.financeportal.v1.GetFinanceWorkspacePreferencesRequest;
import com.acme.financeportal.v1.GetFinanceWorkspacePreferencesResponse;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsRequest;
import com.acme.financeportal.v1.ListFinanceWorkspaceSubscriptionsResponse;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersRequest;
import com.acme.financeportal.v1.NotifyFinanceWorkspaceWatchersResponse;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesRequest;
import com.acme.financeportal.v1.RelayApprovalInboxMessagesResponse;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsRequest;
import com.acme.financeportal.v1.ReplayFinanceWorkspaceEventsResponse;
import com.acme.financeportal.v1.SendFinanceWorkspaceTestNoticeRequest;
import com.acme.financeportal.v1.SendFinanceWorkspaceTestNoticeResponse;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesRequest;
import com.acme.financeportal.v1.StreamFinanceWorkspaceNoticesResponse;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.SubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesRequest;
import com.acme.financeportal.v1.UnsubscribeFinanceWorkspaceUpdatesResponse;
import com.acme.identity.v1.ExplainUserDecisionRequest;
import com.acme.identity.v1.ExplainUserDecisionResponse;
import com.acme.identity.v1.NormalizeUserRequest;
import com.acme.identity.v1.NormalizeUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionWorkflowServiceGrpc;
import com.acme.treasury.v1.ResumeCashPositionWorkflowRequest;
import com.acme.treasury.v1.ResumeCashPositionWorkflowResponse;
import com.google.protobuf.Any;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class FinanceWorkspaceNotificationRequestMapperImpl implements FinanceWorkspaceNotificationRequestMapper {
    @Override
    public ScoreRetentionPolicyRequest subscribeFinanceWorkspaceUpdatesStage0(SubscribeFinanceWorkspaceUpdatesRequest request) {
        return ScoreRetentionPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
    }
}

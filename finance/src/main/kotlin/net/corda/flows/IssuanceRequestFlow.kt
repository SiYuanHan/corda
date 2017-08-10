package net.corda.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.*
import net.corda.core.flows.*
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap
import java.util.*

/**
 * Combined signed transaction and identity lookup map, which is the resulting data from regular cash flows.
 *
 * @param stx the signed transaction.
 * @param recipient the identity used for the other side of the transaction. For anonymous transactions this is the
 * confidential identity generated for the transaction, otherwise this is the well known identity.
 */
@CordaSerializable
data class IssuanceResult(val stx: SignedTransaction, val recipient: AbstractParty)

/**
 * This flow will ask [issuerBank] to issue the given amount of Currency to the node on which this flow is executed on.
 *
 * @param anonymous true if the issued asset should be sent to a new confidential identity, false to send it to the
 * well known identity (generally this is only used in testing).
 */
@InitiatingFlow
@StartableByRPC
class IssuanceRequestFlow(val amount: Amount<Currency>,
                          val issuanceRef: OpaqueBytes,
                          val issuerBank: Party,
                          val notary: Party,
                          val anonymous: Boolean) : FlowLogic<IssuanceResult>() {
    @Suspendable
    @Throws(CashException::class)
    override fun call(): IssuanceResult {
        send(issuerBank, IssuanceRequest(amount, issuanceRef, notary, anonymous))
        val me = serviceHub.myInfo.legalIdentity
        val anonymousMe = if (issuerBank != me && anonymous) {
            // Use anonymous identities if we're not the issuer bank issuing to self and the requester wants anonymity
            subFlow(SwapIdentitiesFlow(issuerBank)).ourIdentity.party
        } else {
            me
        }
        return receive<SignedTransaction>(issuerBank).unwrap { stx ->
            val expectedAmount = Amount(amount.quantity, Issued(issuerBank.ref(issuanceRef), amount.token))
            val cashOutputs = stx.tx.filterOutputs<Cash.State> { it.owner == anonymousMe }
            require(cashOutputs.size == 1) { "Require a single cash output paying $anonymousMe, found ${stx.tx.outputs}" }
            require(cashOutputs.single().amount == expectedAmount) { "Require payment of $expectedAmount"}
            IssuanceResult(stx, anonymousMe)
        }
    }
}

/**
 * Call `MockNode.registerInitiatedFlow(IssuanceRequestHandlerFlow::class.java)` on the node representing the issuer of Currency.
 * This will automatically issue Currency to whoever asks for it. Obviously not for production use, more of a sample.
 */
@InitiatedBy(IssuanceRequestFlow::class)
class IssuanceRequestHandlerFlow(val requester: Party) : FlowLogic<Unit>() {
    companion object {
        object AWAITING_REQUEST : ProgressTracker.Step("Awaiting issuance request")
        object ISSUING : ProgressTracker.Step("Self issuing asset")
        object TRANSFERRING : ProgressTracker.Step("Transferring asset to issuance requester")
        object SENDING_CONFIRM : ProgressTracker.Step("Confirming asset issuance to requester")

        fun tracker() = ProgressTracker(AWAITING_REQUEST, ISSUING, TRANSFERRING, SENDING_CONFIRM)
        private val VALID_CURRENCIES = listOf(USD, GBP, EUR, CHF)
    }

    override val progressTracker: ProgressTracker = tracker()

    @Suspendable
    @Throws(CashException::class)
    override fun call() {
        progressTracker.currentStep = AWAITING_REQUEST
        // TODO: parse request to determine Asset to issue
        val issuanceRequest = receive<IssuanceRequest>(requester).unwrap {
            // validate request inputs (for example, lets restrict the types of currency that can be issued)
            if (it.amount.token !in VALID_CURRENCIES) throw FlowException("Currency must be one of $VALID_CURRENCIES")
            it
        }

        progressTracker.currentStep = ISSUING
        // First self-issue
        val selfIssueStx = subFlow(CashIssueFlow(
                issuanceRequest.amount,
                issuanceRequest.issuerPartyRef,
                serviceHub.myInfo.legalIdentity,
                issuanceRequest.notary))

        val issueStx = if (requester != serviceHub.myInfo.legalIdentity) {
            val anonymousRequester = if (issuanceRequest.anonymous) {
                subFlow(SwapIdentitiesFlow(requester)).theirIdentity.party
            } else {
                requester
            }
            progressTracker.currentStep = TRANSFERRING
            // Now invoke Cash subflow to Move issued assetType to issue requester
            subFlow(CashPaymentFlow(issuanceRequest.amount, anonymousRequester))
        } else {
            // Short-circuit when issuing to self
            selfIssueStx
        }

        progressTracker.currentStep = SENDING_CONFIRM
        send(requester, issueStx)
    }
}

@CordaSerializable
private data class IssuanceRequest(val amount: Amount<Currency>,
                                   val issuerPartyRef: OpaqueBytes,
                                   val notary: Party,
                                   val anonymous: Boolean)

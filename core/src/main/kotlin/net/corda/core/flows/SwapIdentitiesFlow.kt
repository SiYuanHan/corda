package net.corda.core.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.core.identity.AnonymousPartyAndPath
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap

/**
 * Flow for swapping anonymous identities with a counter-party involved in the creation of a new transaction. The anonymous
 * identities created will be stored in each party's identity store with the mapping to their known identities.
 *
 * This is to be executed within both the flows dealing with the creation of the new transaction as a sub-flow call.
 */
class SwapIdentitiesFlow(val otherSide: Party,
                         val revocationEnabled: Boolean,
                         override val progressTracker: ProgressTracker) : FlowLogic<SwapIdentitiesFlow.Result>() {
    constructor(otherSide: Party) : this(otherSide, false, tracker())

    companion object {
        object GENERATING_ID : ProgressTracker.Step("Generating anonymous identity")
        object AWAITING_ID : ProgressTracker.Step("Awaiting counter-party's anonymous identity")

        fun tracker() = ProgressTracker(GENERATING_ID, AWAITING_ID)
    }

    @Suspendable
    override fun call(): Result {
        progressTracker.currentStep = GENERATING_ID
        val ourIdentity = serviceHub.keyManagementService.freshKeyAndCert(serviceHub.myInfo.legalIdentityAndCert, revocationEnabled)

        println("about to send our identity and wait for other side...")
        val theirIdentity = if (otherSide != serviceHub.myInfo.legalIdentity) {
            progressTracker.currentStep = AWAITING_ID
            sendAndReceive<AnonymousPartyAndPath>(otherSide, ourIdentity).unwrap {
                serviceHub.identityService.verifyAndRegisterAnonymousIdentity(it, otherSide)
                it
            }
        } else {
            // Special case that if we're both parties, a single identity is generated
            ourIdentity
        }
        println("... received other side's identity")

        return Result(ourIdentity, theirIdentity)
    }

    @CordaSerializable
    data class Result(val ourIdentity: AnonymousPartyAndPath, val theirIdentity: AnonymousPartyAndPath)
}


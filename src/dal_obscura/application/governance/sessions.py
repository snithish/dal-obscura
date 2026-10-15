"""Application ownership of durable browser and login transactions."""

from __future__ import annotations

from dataclasses import dataclass

from dal_obscura.application.governance.ports import GovernanceUnitOfWorkFactory
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from dal_obscura.kernel.identity.session import LoginRateLimitDecision, LoginTransaction

MAX_BROWSER_SESSION_TTL_SECONDS = 86_400
MAX_BROWSER_IDLE_TTL_SECONDS = 7_200


@dataclass(frozen=True)
class SessionCommands:
    work_factory: GovernanceUnitOfWorkFactory
    session_ttl_seconds: int = 28_800
    session_idle_ttl_seconds: int = 1_800
    login_rate_limit_attempts: int = 20
    login_rate_limit_window_seconds: int = 60
    login_rate_limit_block_seconds: int = 300
    login_rate_limit_aggregate_attempts: int = 200

    def __post_init__(self) -> None:
        if not 1 <= self.session_ttl_seconds <= MAX_BROWSER_SESSION_TTL_SECONDS:
            raise ValueError("session TTL must be between 1 second and 24 hours")
        if not 1 <= self.session_idle_ttl_seconds <= MAX_BROWSER_IDLE_TTL_SECONDS:
            raise ValueError("idle session TTL must be between 1 second and 2 hours")
        if self.session_idle_ttl_seconds > self.session_ttl_seconds:
            raise ValueError("idle session TTL cannot exceed session TTL")
        if self.login_rate_limit_aggregate_attempts <= 0:
            raise ValueError("aggregate login rate-limit attempts must be positive")

    def resolve_browser_session(
        self,
        token: str,
        csrf_token: str | None = None,
    ) -> ControlPlaneActor | None:
        """Loads an unexpired browser session without exposing provider tokens."""

        with self.work_factory() as work:
            actor = work.browser_sessions.resolve(
                token,
                csrf_token=csrf_token,
                idle_ttl_seconds=self.session_idle_ttl_seconds,
            )
            if actor is not None:
                work.commit()
            return actor

    def issue_browser_session_credentials(self, actor: ControlPlaneActor) -> tuple[str, str]:
        """Mints the session and its browser-readable, session-bound CSRF secret."""

        with self.work_factory() as work:
            credentials = work.browser_sessions.issue_with_csrf(
                actor,
                ttl_seconds=self.session_ttl_seconds,
            )
            work.commit()
            return credentials

    def revoke_browser_session(self, token: str) -> None:
        """Revokes a browser session secret, if it exists."""

        with self.work_factory() as work:
            work.browser_sessions.revoke(token)
            work.commit()

    def issue_login_transaction(
        self,
        *,
        state: str,
        nonce: str,
        code_verifier: str,
        redirect_uri: str,
    ) -> None:
        with self.work_factory() as work:
            work.login_transactions.issue(
                state=state,
                nonce=nonce,
                code_verifier=code_verifier,
                redirect_uri=redirect_uri,
            )
            work.commit()

    def check_login_rate_limit(
        self,
        client_key: str,
        *,
        aggregate_key: str | None = None,
    ) -> LoginRateLimitDecision:
        """Records a login start and returns the generic admission decision."""

        with self.work_factory() as work:
            limiter = work.login_rate_limits
            decision = limiter.allow(
                client_key,
                max_attempts=self.login_rate_limit_attempts,
                window_seconds=self.login_rate_limit_window_seconds,
                block_seconds=self.login_rate_limit_block_seconds,
            )
            if decision.allowed and aggregate_key and aggregate_key != client_key:
                decision = limiter.allow(
                    aggregate_key,
                    max_attempts=self.login_rate_limit_aggregate_attempts,
                    window_seconds=self.login_rate_limit_window_seconds,
                    block_seconds=self.login_rate_limit_block_seconds,
                )
            work.commit()
            return decision

    def record_login_failure(
        self,
        client_key: str,
        *,
        aggregate_key: str | None = None,
    ) -> LoginRateLimitDecision:
        """Records a failed callback using the same durable client window."""

        with self.work_factory() as work:
            limiter = work.login_rate_limits
            decision = limiter.record_failure(
                client_key,
                max_attempts=self.login_rate_limit_attempts,
                window_seconds=self.login_rate_limit_window_seconds,
                block_seconds=self.login_rate_limit_block_seconds,
            )
            if decision.allowed and aggregate_key and aggregate_key != client_key:
                decision = limiter.record_failure(
                    aggregate_key,
                    max_attempts=self.login_rate_limit_aggregate_attempts,
                    window_seconds=self.login_rate_limit_window_seconds,
                    block_seconds=self.login_rate_limit_block_seconds,
                )
            work.commit()
            return decision

    def clear_login_rate_limit(self, client_key: str) -> None:
        """Clears login abuse state after a successful callback."""

        with self.work_factory() as work:
            work.login_rate_limits.clear(client_key)
            work.commit()

    def consume_login_transaction(self, state: str) -> LoginTransaction | None:
        with self.work_factory() as work:
            transaction = work.login_transactions.consume(state)
            work.commit()
            return transaction

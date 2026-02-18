"""Response converters for model-to-schema transformations.

This module provides centralized converter functions for converting
database models to Pydantic response schemas. Eliminates duplication
of conversion logic across routers.
"""

from apps.core.models import Filter, LogSource, Rule, User
from apps.core.schemas import (
    FilterActiveResponse,
    FilterDetailResponse,
    FilterFullResponse,
    LogSourceResponse,
    RuleDetailResponse,
    RuleFullDetailResponse,
    UserDetailResponse,
)


class RuleConverter:
    """Converter for Rule model to response schemas."""

    @staticmethod
    def to_detail(rule: Rule) -> RuleDetailResponse:
        """Convert Rule to RuleDetailResponse."""
        return RuleDetailResponse(
            id=str(rule.id),
            name=rule.name,
            repository_id=str(rule.repository_id),
            repository_type=rule.repository.type if rule.repository else "",
            repository_name=rule.repository.name if rule.repository else "",
            product=rule.product if rule.product else None,
            service=rule.service if rule.service else None,
            category=rule.category if rule.category else None,
            created=rule.created,
            updated=rule.updated,
        )

    @staticmethod
    def to_full_detail(rule: Rule) -> RuleFullDetailResponse:
        """Convert Rule to RuleFullDetailResponse (includes body)."""
        detail = RuleConverter.to_detail(rule)
        return RuleFullDetailResponse(
            **detail.model_dump(),
            body=rule.body,
        )


class UserConverter:
    """Converter for User model to response schemas."""

    @staticmethod
    def to_detail(user: User) -> UserDetailResponse:
        """Convert User to UserDetailResponse."""
        return UserDetailResponse(
            id=str(user.id),
            full_name=user.full_name,
            email=user.email,
            is_active=user.is_active,
            role=user.role,
            must_change_password=user.must_change_password,
            created=user.created,
            updated=user.updated,
        )


class FilterConverter:
    """Converter for Filter model to response schemas."""

    @staticmethod
    def to_active(filter_obj: Filter) -> FilterActiveResponse:
        """Convert Filter to FilterActiveResponse (id and name only)."""
        return FilterActiveResponse(
            id=str(filter_obj.id),
            name=filter_obj.name,
        )

    @staticmethod
    def to_detail(filter_obj: Filter) -> FilterDetailResponse:
        """Convert Filter to FilterDetailResponse."""
        return FilterDetailResponse(
            id=str(filter_obj.id),
            name=filter_obj.name,
            created=filter_obj.created.isoformat() if filter_obj.created else "",
            updated=filter_obj.updated.isoformat() if filter_obj.updated else "",
        )

    @staticmethod
    def to_full(filter_obj: Filter) -> FilterFullResponse:
        """Convert Filter to FilterFullResponse (includes body)."""
        return FilterFullResponse(
            id=str(filter_obj.id),
            name=filter_obj.name,
            body=filter_obj.body,
            created=filter_obj.created.isoformat() if filter_obj.created else "",
            updated=filter_obj.updated.isoformat() if filter_obj.updated else "",
        )


class LogSourceConverter:
    """Converter for LogSource model to response schemas."""

    @staticmethod
    def to_response(log_source: LogSource) -> LogSourceResponse:
        """Convert LogSource to LogSourceResponse."""
        return LogSourceResponse(
            id=str(log_source.id),
            name=log_source.name,
            parsing_script=log_source.parsing_script,
            parsing_config=log_source.parsing_config,
            mapping=log_source.mapping,
            test_topics=log_source.test_topics,
            test_repository_ids=[str(rid) for rid in log_source.test_repository_ids]
            if log_source.test_repository_ids
            else None,
            created=log_source.created.isoformat() if log_source.created else "",
            updated=log_source.updated.isoformat() if log_source.updated else "",
        )

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from api.db import Base


class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(String(254), nullable=False, index=True)
    postal_code: Mapped[str] = mapped_column(String(3), nullable=False)
    store_codes: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    unsubscribe_token: Mapped[uuid.UUID] = mapped_column(
        nullable=False, default=uuid.uuid4, unique=True
    )
    consented_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sent_week: Mapped[str | None] = mapped_column(String(8), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

import os
import time
from datetime import datetime, timezone
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://iot_user:iot_pass@postgres:5432/hospital_iot",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Device(Base):
    __tablename__ = "devices"

    device_id = Column(String, primary_key=True, index=True)
    device_name = Column(String, nullable=False)
    location = Column(String, nullable=False)
    registered_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    telemetry_logs = relationship("TelemetryLog", back_populates="device")


class TelemetryLog(Base):
    __tablename__ = "telemetry_logs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    device_id = Column(String, ForeignKey("devices.device_id"), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    temperature = Column(Float, nullable=False)
    battery_percentage = Column(Float, nullable=False)
    status = Column(String, nullable=True)

    device = relationship("Device", back_populates="telemetry_logs")


def init_db(retries: int = 15, delay: float = 2.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            Base.metadata.create_all(bind=engine)
            with SessionLocal() as db:
                existing = db.query(Device).filter(Device.device_id == "node_01").first()
                if not existing:
                    db.add(
                        Device(
                            device_id="node_01",
                            device_name="Infusion Pump",
                            location="Ward A - Room 101",
                        )
                    )
                    db.commit()
            return
        except OperationalError:
            if attempt == retries:
                raise
            time.sleep(delay)
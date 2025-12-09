from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from pydantic import BaseModel
from typing import List, Optional
import random
from datetime import datetime

# Database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Models
class Operator(Base):
    __tablename__ = "tbl_operators"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    is_active = Column(Boolean, default=True)
    max_load = Column(Integer, default=10)
    operator_sources = relationship("OperatorSource", back_populates="operator")
    appeals = relationship("Appeal", back_populates="operator")


class Source(Base):
    __tablename__ = "tbl_sources"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    operator_sources = relationship("OperatorSource", back_populates="source")


class OperatorSource(Base):
    __tablename__ = "tbl_operator_sources"
    id = Column(Integer, primary_key=True, index=True)
    operator_id = Column(Integer, ForeignKey("tbl_operators.id"))
    source_id = Column(Integer, ForeignKey("tbl_sources.id"))
    weight = Column(Integer, default=0)
    operator = relationship("Operator", back_populates="operator_sources")
    source = relationship("Source", back_populates="operator_sources")


class Lead(Base):
    __tablename__ = "tbl_leads"
    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True)
    appeals = relationship("Appeal", back_populates="lead")


class Appeal(Base):
    __tablename__ = "tbl_appeals"
    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, ForeignKey("tbl_leads.id"))
    source_id = Column(Integer, ForeignKey("tbl_sources.id"))
    operator_id = Column(Integer, ForeignKey("tbl_operators.id"), nullable=True)
    timestamp = Column(String)
    message = Column(String, nullable=True)
    lead = relationship("Lead", back_populates="appeals")
    source = relationship("Source")
    operator = relationship("Operator", back_populates="appeals")


Base.metadata.create_all(bind=engine)


# Pydantic models
class OperatorCreate(BaseModel):
    name: str
    is_active: bool = True
    max_load: int = 10


class OperatorUpdate(BaseModel):
    is_active: Optional[bool] = None
    max_load: Optional[int] = None


class OperatorOut(BaseModel):
    id: int
    name: str
    is_active: bool
    max_load: int

    class Config:
        orm_mode = True


class SourceCreate(BaseModel):
    name: str


class SourceOut(BaseModel):
    id: int
    name: str

    class Config:
        orm_mode = True


class OperatorSourceCreate(BaseModel):
    operator_id: int
    source_id: int
    weight: int


class OperatorSourceOut(BaseModel):
    id: int
    operator_id: int
    source_id: int
    weight: int

    class Config:
        orm_mode = True


class AppealCreate(BaseModel):
    external_id: str
    source_id: int
    message: Optional[str] = None


class AppealOut(BaseModel):
    id: int
    lead_id: int
    source_id: int
    operator_id: Optional[int]
    timestamp: str
    message: Optional[str]

    class Config:
        orm_mode = True


class LeadOut(BaseModel):
    id: int
    external_id: str
    appeals: List[AppealOut]

    class Config:
        orm_mode = True


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


app = FastAPI()


# Operators
@app.post("/operators/", response_model=OperatorOut)
def create_operator(operator: OperatorCreate, db: Session = Depends(get_db)):
    db_operator = Operator(**operator.model_dump())
    db.add(db_operator)
    db.commit()
    db.refresh(db_operator)
    return db_operator


@app.get("/operators/", response_model=List[OperatorOut])
def list_operators(db: Session = Depends(get_db)):
    return db.query(Operator).all()


@app.patch("/operators/{operator_id}", response_model=OperatorOut)
def update_operator(operator_id: int, operator: OperatorUpdate, db: Session = Depends(get_db)):
    db_operator = db.query(Operator).filter(Operator.id == operator_id).first()
    if not db_operator:
        raise HTTPException(status_code=404, detail="Operator not found")
    update_data = operator.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_operator, key, value)
    db.commit()
    db.refresh(db_operator)
    return db_operator


# Sources
@app.post("/sources/", response_model=SourceOut)
def create_source(source: SourceCreate, db: Session = Depends(get_db)):
    db_source = Source(**source.model_dump())
    db.add(db_source)
    db.commit()
    db.refresh(db_source)
    return db_source


@app.get("/sources/", response_model=List[SourceOut])
def list_sources(db: Session = Depends(get_db)):
    return db.query(Source).all()


# Operator Sources
@app.post("/operator-sources/", response_model=OperatorSourceOut)
def create_operator_source(os: OperatorSourceCreate, db: Session = Depends(get_db)):
    db_os = OperatorSource(**os.model_dump())
    db.add(db_os)
    db.commit()
    db.refresh(db_os)
    return db_os


@app.get("/operator-sources/", response_model=List[OperatorSourceOut])
def list_operator_sources(source_id: Optional[int] = None, db: Session = Depends(get_db)):
    query = db.query(OperatorSource)
    if source_id:
        query = query.filter(OperatorSource.source_id == source_id)
    return query.all()


# Appeals
@app.post("/appeals/", response_model=AppealOut)
def create_appeal(appeal: AppealCreate, db: Session = Depends(get_db)):
    # Find or create lead
    db_lead = db.query(Lead).filter(Lead.external_id == appeal.external_id).first()
    if not db_lead:
        db_lead = Lead(external_id=appeal.external_id)
        db.add(db_lead)
        db.commit()
        db.refresh(db_lead)

    # Find available operators for source
    subquery = db.query(Appeal.operator_id, func.count(Appeal.id).label('load')).group_by(Appeal.operator_id).subquery()
    candidates = db.query(Operator, OperatorSource.weight).join(OperatorSource,
                                                                Operator.id == OperatorSource.operator_id).outerjoin(
        subquery, subquery.c.operator_id == Operator.id).filter(
        OperatorSource.source_id == appeal.source_id,
        Operator.is_active == True,
        (subquery.c.load < Operator.max_load) | (subquery.c.load.is_(None))
    ).all()

    operator_id = None
    if candidates:
        operators, weights = zip(*[(op.id, w) for op, w in candidates if w > 0])
        total_weight = sum(weights)
        if total_weight > 0:
            chosen_weight = random.choices(weights, weights=weights, k=1)[0]
            index = weights.index(chosen_weight)
            operator_id = operators[index]

    # Create appeal
    db_appeal = Appeal(
        lead_id=db_lead.id,
        source_id=appeal.source_id,
        operator_id=operator_id,
        timestamp=datetime.now().isoformat(),
        message=appeal.message
    )
    db.add(db_appeal)
    db.commit()
    db.refresh(db_appeal)
    return db_appeal


# Views
@app.get("/leads/", response_model=List[LeadOut])
def list_leads(db: Session = Depends(get_db)):
    return db.query(Lead).all()


@app.get("/appeals/", response_model=List[AppealOut])
def list_appeals(db: Session = Depends(get_db)):
    return db.query(Appeal).all()

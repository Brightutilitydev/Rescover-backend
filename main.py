import os
import uuid
from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import FastAPI, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
import bcrypt
import jwt
from motor.motor_asyncio import AsyncIOMotorClient

# --- 1. CONFIGURATION ---
SECRET_KEY = "rescover-super-secret-key-2026"
ALGORITHM = "HS256"
MONGO_URL = os.getenv("MONGO_URL","mongodb+srv://masterbright02_db_user:Bright2026@rescover.ymtcvni.mongodb.net/?appName=Rescover")

app = FastAPI(title="Rescover API - Dynamic Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://rescover-frontend.vercel.app/"], # Changed to allow any frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 2. DATABASE CONNECTION ---
class DatabaseHelper:
    client: AsyncIOMotorClient = None
    db = None

db_helper = DatabaseHelper()

@app.on_event("startup")
async def connect_to_mongo():
    try:
        db_helper.client = AsyncIOMotorClient(MONGO_URL, serverSelectionTimeoutMS=2000)
        await db_helper.client.admin.command('ping')
        db_helper.db = db_helper.client.rescover_db
        print("✅ SUCCESS: MongoDB is Connected and Ready!")
   except Exception as e:
        print(f"❌ ERROR: MONGODB IS NOT RUNNING! The real reason is: {e}")
        db_helper.db = None

# --- 3. SECURITY & DATA MODELS ---
class UserRegister(BaseModel):
    fullname: str = Field(..., min_length=3)
    email: EmailStr
    password: str = Field(..., min_length=6)

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class PaperCreate(BaseModel):
    title: str
    author_name: str

class SectionData(BaseModel):
    id: str
    title: str
    content: str

class PaperUpdate(BaseModel):
    title: str
    sections: List[SectionData]
    user_fullname: str 

class NewSection(BaseModel):
    title: str
    user_fullname: str

class DeleteSection(BaseModel):
    user_fullname: str

class RemoveAuthor(BaseModel):
    user_fullname: str
    admin_name: str

class ChatMessage(BaseModel):
    author: str
    text: str

class LockStatus(BaseModel):
    section: str
    user_fullname: Optional[str] = None

class ACLRequest(BaseModel):
    user_fullname: str

def get_password_hash(password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

def create_token(data: dict) -> str:
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + timedelta(hours=24)})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# --- 4. WEBSOCKET CONNECTION MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, list[dict]] = {}

    async def connect(self, websocket: WebSocket, paper_id: str, user_fullname: str):
        await websocket.accept()
        if paper_id not in self.active_connections:
            self.active_connections[paper_id] = []
        self.active_connections[paper_id].append({"ws": websocket, "user": user_fullname})
        await self.broadcast_presence(paper_id)

    def disconnect(self, websocket: WebSocket, paper_id: str):
        if paper_id in self.active_connections:
            self.active_connections[paper_id] = [c for c in self.active_connections[paper_id] if c["ws"] != websocket]
            if not self.active_connections[paper_id]:
                del self.active_connections[paper_id]

    async def broadcast_presence(self, paper_id: str):
        if paper_id in self.active_connections:
            users = list(set([c["user"] for c in self.active_connections[paper_id]]))
            for connection in self.active_connections[paper_id]:
                try: await connection["ws"].send_json({"type": "presence", "users": users})
                except Exception: pass

    async def broadcast_refresh(self, paper_id: str, sender_ws: Optional[WebSocket] = None):
        if paper_id in self.active_connections:
            for connection in self.active_connections[paper_id]:
                if connection["ws"] != sender_ws:
                    try: await connection["ws"].send_json({"type": "refresh"})
                    except Exception: pass

manager = ConnectionManager()

@app.websocket("/ws/{paper_id}")
async def websocket_endpoint(websocket: WebSocket, paper_id: str, user: str):
    await manager.connect(websocket, paper_id, user)
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "refresh":
                await manager.broadcast_refresh(paper_id, websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket, paper_id)
        await manager.broadcast_presence(paper_id)

# --- 5. AUTH ENDPOINTS ---
@app.post("/api/auth/register")
async def register(user: UserRegister):
    if db_helper.db is None: raise HTTPException(status_code=500)
    if await db_helper.db.users.find_one({"email": user.email}): raise HTTPException(status_code=400)
    await db_helper.db.users.insert_one({"fullname": user.fullname, "email": user.email, "password": get_password_hash(user.password)})
    return {"msg": "Registered successfully."}

@app.post("/api/auth/login")
async def login(user: UserLogin):
    if db_helper.db is None: raise HTTPException(status_code=500)
    db_user = await db_helper.db.users.find_one({"email": user.email})
    if not db_user or not verify_password(user.password, db_user["password"]): raise HTTPException(status_code=401)
    return {"access_token": create_token({"sub": db_user["email"]}), "fullname": db_user["fullname"], "role": "researcher"}

# --- 6. DASHBOARD & ACL ENDPOINTS ---
@app.post("/api/papers/create")
async def create_paper(paper: PaperCreate):
    if db_helper.db is None: raise HTTPException(status_code=500)
    
    initial_sections = [
        {"id": "abstract", "title": "Abstract", "content": ""},
        {"id": "chapter_1", "title": "Chapter 1: Introduction", "content": ""},
        {"id": "chapter_2", "title": "Chapter 2: Literature Review", "content": ""}
    ]
    
    paper_record = {
        "id": str(uuid.uuid4()), "title": paper.title, 
        "sections": initial_sections,
        "status": "drafting", "author_name": paper.author_name, "owner_name": paper.author_name,
        "co_authors": [], "pending_requests": [],
        "title_locked_by": None, "abstract_locked_by": None, "chapter_1_locked_by": None, "chapter_2_locked_by": None,
        "chat": [], "audit_log": [], "plagiarism_score": 0.0, "comments": []
    }
    await db_helper.db.papers.insert_one(paper_record)
    return {"msg": "Paper created successfully", "paper": paper_record}

@app.get("/api/papers")
async def get_papers(user_name: str = ""):
    if db_helper.db is None: raise HTTPException(status_code=500)
    papers = await db_helper.db.papers.find({}, {"_id": 0}).to_list(length=100)
    my_drafts, discover, in_review, published = [], [], [], []
    for p in papers:
        status, owner, co_authors = p.get("status"), p.get("owner_name", ""), p.get("co_authors", [])
        if status == "drafting":
            if owner == user_name or user_name in co_authors: my_drafts.append(p)
            else: discover.append(p)
        elif status == "in_review": in_review.append(p)
        elif status == "published": published.append(p)
    return {"drafts": my_drafts, "discover": discover, "in_review": in_review, "published": published}

@app.post("/api/papers/{paper_id}/request-join")
async def request_join(paper_id: str, request: ACLRequest):
    if db_helper.db is None: raise HTTPException(status_code=500)
    paper = await db_helper.db.papers.find_one({"id": paper_id})
    if paper:
        await db_helper.db.papers.update_one({"id": paper_id}, {"$addToSet": {"pending_requests": request.user_fullname}})
        await db_helper.db.notifications.insert_one({
            "id": str(uuid.uuid4()), "user_fullname": paper["owner_name"],
            "message": f"New Co-Author Request: {request.user_fullname} wants to join '{paper['title']}'",
            "is_read": False, "time": datetime.utcnow().strftime("%H:%M")
        })
    return {"msg": "Join request sent."}

@app.post("/api/papers/{paper_id}/approve-join")
async def approve_join(paper_id: str, request: ACLRequest):
    if db_helper.db is None: raise HTTPException(status_code=500)
    paper = await db_helper.db.papers.find_one({"id": paper_id})
    if paper:
        await db_helper.db.papers.update_one({"id": paper_id}, {"$pull": {"pending_requests": request.user_fullname}, "$addToSet": {"co_authors": request.user_fullname}})
        await db_helper.db.notifications.insert_one({
            "id": str(uuid.uuid4()), "user_fullname": request.user_fullname,
            "message": f"Approved! You are now a co-author on '{paper['title']}'",
            "is_read": False, "time": datetime.utcnow().strftime("%H:%M")
        })
    return {"msg": "Co-author approved."}

# --- 7. NOTIFICATIONS ENDPOINTS ---
@app.get("/api/notifications/{user_fullname}")
async def get_notifications(user_fullname: str):
    if db_helper.db is None: return []
    notifs = await db_helper.db.notifications.find({"user_fullname": user_fullname}, {"_id": 0}).to_list(None)
    return notifs[::-1] 

@app.put("/api/notifications/{notif_id}/read")
async def mark_notification_read(notif_id: str):
    if db_helper.db is not None:
        await db_helper.db.notifications.update_one({"id": notif_id}, {"$set": {"is_read": True}})
    return {"msg": "Marked read"}

# --- 8. DYNAMIC WORKBENCH ENDPOINTS ---
@app.get("/api/papers/{paper_id}")
async def get_single_paper(paper_id: str):
    if db_helper.db is None: raise HTTPException(status_code=500)
    paper = await db_helper.db.papers.find_one({"id": paper_id}, {"_id": 0})
    if not paper: raise HTTPException(status_code=404)
    if "sections" not in paper:
        paper["sections"] = [
            {"id": "abstract", "title": "Abstract", "content": paper.get("abstract", "")},
            {"id": "chapter_1", "title": "Chapter 1: Introduction", "content": paper.get("chapter_1", "")},
            {"id": "chapter_2", "title": "Chapter 2: Literature Review", "content": paper.get("chapter_2", "")}
        ]
    return paper

@app.put("/api/papers/{paper_id}/sync")
async def sync_paper(paper_id: str, update_data: PaperUpdate):
    if db_helper.db is None: raise HTTPException(status_code=500)
    
    audit_entry = {"id": str(uuid.uuid4()), "action": "Synced document sections.", "user": update_data.user_fullname, "time": datetime.utcnow().strftime("%H:%M")}
    
    await db_helper.db.papers.update_one(
        {"id": paper_id},
        {"$set": {"title": update_data.title, "sections": [s.dict() for s in update_data.sections]},
         "$push": {"audit_log": audit_entry}}
    )
    return {"msg": "Workspace synced successfully."}

@app.post("/api/papers/{paper_id}/sections")
async def add_custom_section(paper_id: str, section_data: NewSection):
    if db_helper.db is None: raise HTTPException(status_code=500)
    
    sec_id = f"sec_{uuid.uuid4().hex[:8]}"
    new_sec = {"id": sec_id, "title": section_data.title, "content": ""}
    audit_entry = {"id": str(uuid.uuid4()), "action": f"Added section: {section_data.title}", "user": section_data.user_fullname, "time": datetime.utcnow().strftime("%H:%M")}
    
    await db_helper.db.papers.update_one(
        {"id": paper_id},
        {"$push": {"sections": new_sec, "audit_log": audit_entry},
         "$set": {f"{sec_id}_locked_by": None}}
    )
    await manager.broadcast_refresh(paper_id)
    return {"msg": "Section added."}

@app.post("/api/papers/{paper_id}/sections/{section_id}/delete")
async def delete_custom_section(paper_id: str, section_id: str, payload: DeleteSection):
    if db_helper.db is None: raise HTTPException(status_code=500)
    
    audit_entry = {"id": str(uuid.uuid4()), "action": "Deleted a manuscript section.", "user": payload.user_fullname, "time": datetime.utcnow().strftime("%H:%M")}
    
    await db_helper.db.papers.update_one(
        {"id": paper_id},
        {"$pull": {"sections": {"id": section_id}},
         "$push": {"audit_log": audit_entry}}
    )
    await manager.broadcast_refresh(paper_id)
    return {"msg": "Section deleted."}

@app.post("/api/papers/{paper_id}/remove-coauthor")
async def remove_coauthor(paper_id: str, payload: RemoveAuthor):
    if db_helper.db is None: raise HTTPException(status_code=500)
    
    audit_entry = {"id": str(uuid.uuid4()), "action": f"Revoked access for: {payload.user_fullname}", "user": payload.admin_name, "time": datetime.utcnow().strftime("%H:%M")}
    
    await db_helper.db.papers.update_one(
        {"id": paper_id},
        {"$pull": {"co_authors": payload.user_fullname},
         "$push": {"audit_log": audit_entry}}
    )
    
    await db_helper.db.notifications.insert_one({
        "id": str(uuid.uuid4()), "user_fullname": payload.user_fullname,
        "message": f"Your co-author access to a paper was revoked by {payload.admin_name}.",
        "is_read": False, "time": datetime.utcnow().strftime("%H:%M")
    })
    await manager.broadcast_refresh(paper_id)
    return {"msg": "Author removed."}

@app.post("/api/papers/{paper_id}/lock")
async def toggle_lock(paper_id: str, lock_data: LockStatus):
    if db_helper.db is None: raise HTTPException(status_code=500)
    action_text = f"Claimed lock on {lock_data.section}" if lock_data.user_fullname else f"Released lock on {lock_data.section}"
    user_name = lock_data.user_fullname if lock_data.user_fullname else "System"
    audit_entry = {"id": str(uuid.uuid4()), "action": action_text, "user": user_name, "time": datetime.utcnow().strftime("%H:%M")}
    
    await db_helper.db.papers.update_one(
        {"id": paper_id}, 
        {"$set": {f"{lock_data.section}_locked_by": lock_data.user_fullname},
         "$push": {"audit_log": audit_entry}}
    )
    return {"msg": "Lock status updated."}

@app.post("/api/papers/{paper_id}/chat")
async def add_chat_message(paper_id: str, message: ChatMessage):
    if db_helper.db is None: raise HTTPException(status_code=500)
    chat_entry = {"id": str(uuid.uuid4()), "author": message.author, "text": message.text, "time": datetime.utcnow().strftime("%H:%M")}
    await db_helper.db.papers.update_one({"id": paper_id}, {"$push": {"chat": chat_entry}})
    return chat_entry

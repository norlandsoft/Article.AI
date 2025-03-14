from fastapi import APIRouter, Request, HTTPException

router = APIRouter()

@router.post("/extract")
async def extract(request: Request):
    data = await request.json()
    print(data.get("id"))
    return {"success": True, "message": "ok"}

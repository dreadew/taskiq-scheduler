from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["Health Check"])


@router.get("")
async def get_result():
    return {"status": "ok"}

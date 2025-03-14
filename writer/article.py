import sys

# 将项目根目录添加到sys.path
sys.path.append('../')


from fastapi import FastAPI
from writer.web.platform_router import router as platform_router


app = FastAPI()
app.include_router(platform_router, prefix="/api/platform", tags=["platform"])
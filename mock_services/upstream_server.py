import argparse
import asyncio
import time
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import uvicorn


def create_mock_app(service_id: str, is_fallback: bool = False):
    """Factory to create a mock upstream backend server."""
    app = FastAPI(title=f"Mock Upstream: {service_id}")
    
    # State flags for dynamic testing
    state = {
        "should_fail": False,
        "failure_status": 503,
        "artificial_delay_ms": 0.0,
        "request_count": 0,
    }

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
    async def handle_mock_request(path: str, request: Request):
        state["request_count"] += 1
        
        # Inject artificial delay if configured
        if state["artificial_delay_ms"] > 0:
            await asyncio.sleep(state["artificial_delay_ms"] / 1000.0)

        # Inject failure if configured
        if state["should_fail"]:
            raise HTTPException(status_code=state["failure_status"], detail=f"Simulated upstream outage on {service_id}")

        body = None
        try:
            body = await request.json()
        except Exception:
            pass

        # OpenAI chat completion mock response
        if "chat/completions" in path:
            model = body.get("model", "mock-model") if body else "mock-model"
            return {
                "id": f"chatcmpl-{service_id}-{int(time.time())}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model,
                "provider": service_id,
                "is_fallback": is_fallback,
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": f"Hello from {service_id}! (Model: {model})",
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 12, "completion_tokens": 10, "total_tokens": 22},
            }

        return {
            "service_id": service_id,
            "path": f"/{path}",
            "is_fallback": is_fallback,
            "method": request.method,
            "received_body": body,
            "request_num": state["request_count"],
        }

    @app.post("/__control/fail")
    async def set_failure(fail: bool = True, status_code: int = 503):
        state["should_fail"] = fail
        state["failure_status"] = status_code
        return {"service_id": service_id, "should_fail": fail, "status_code": status_code}

    @app.post("/__control/delay")
    async def set_delay(delay_ms: float = 0.0):
        state["artificial_delay_ms"] = delay_ms
        return {"service_id": service_id, "delay_ms": delay_ms}

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock Upstream Service")
    parser.add_argument("--id", type=str, default="upstream-1", help="Service identifier")
    parser.add_argument("--port", type=int, default=9001, help="Port to bind")
    parser.add_argument("--fallback", action="store_true", help="Flag if this is a fallback service")
    args = parser.parse_args()

    mock_app = create_mock_app(args.id, is_fallback=args.fallback)
    uvicorn.run(mock_app, host="127.0.0.1", port=args.port, log_level="warning")

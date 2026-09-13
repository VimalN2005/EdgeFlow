import json
import time
import httpx
import streamlit as st

st.set_page_config(
    page_title="EdgeFlow Control Plane & Gateway Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

GATEWAY_BASE_URL = "http://localhost:8000"

st.title("⚡ EdgeFlow: Intelligent Traffic Gateway & Control Plane")
st.markdown("Dynamic routing, multi-tier rate limiting, deterministic response caching, and model failover.")

# Sidebar Configuration
st.sidebar.header("Gateway Configuration")
gateway_url = st.sidebar.text_input("Gateway Base URL", value=GATEWAY_BASE_URL)
api_key = st.sidebar.selectbox(
    "Select API Key / Tier",
    options=["ef-live-admin-key (Enterprise)", "ef-live-pro-key (Pro)", "ef-live-free-key (Free)", "None (Anonymous)"],
    index=0,
)
selected_key = api_key.split(" ")[0] if "None" not in api_key else ""

# Headers setup
headers = {"Content-Type": "application/json"}
if selected_key:
    headers["X-API-Key"] = selected_key

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["🚀 Request Playground", "🛡️ Resilience & Failover", "📊 Routes & Circuits", "📈 Prometheus Telemetry"])

# --- TAB 1: Request Playground ---
with tab1:
    st.subheader("Model Inference & Gateway Proxy Playground")
    col1, col2 = st.columns([1, 1])

    with col1:
        endpoint = st.selectbox(
            "Target Endpoint",
            ["/v1/chat/completions", "/healthz", "/readyz", "/v1/models"],
        )
        model = st.text_input("Model Alias", value="gpt-4o")
        prompt = st.text_area("Prompt / Query", value="Explain EdgeFlow architecture in two bullet points.")
        bypass_cache = st.checkbox("Bypass Cache (Cache-Control: no-cache)")

        req_headers = dict(headers)
        if bypass_cache:
            req_headers["Cache-Control"] = "no-cache"

        send_btn = st.button("Send Request ⚡", type="primary", use_container_width=True)

    with col2:
        if send_btn:
            url = f"{gateway_url}{endpoint}"
            t0 = time.perf_counter()
            try:
                if endpoint == "/v1/chat/completions":
                    payload = {
                        "model": model,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                    resp = httpx.post(url, json=payload, headers=req_headers, timeout=10.0)
                else:
                    resp = httpx.get(url, headers=req_headers, timeout=10.0)

                latency_ms = (time.perf_counter() - t0) * 1000.0

                # Metrics callouts
                mcol1, mcol2, mcol3, mcol4 = st.columns(4)
                mcol1.metric("Status", f"{resp.status_code}")
                mcol2.metric("Total Latency", f"{latency_ms:.1f} ms")
                cache_status = resp.headers.get("X-Cache", "NONE")
                mcol3.metric("Cache Status", cache_status)
                target = resp.headers.get("X-EdgeFlow-Target", "N/A")
                mcol4.metric("Routed Upstream", target)

                if resp.status_code == 200:
                    st.success("Request Dispatched Successfully!")
                    st.json(resp.json())
                elif resp.status_code == 429:
                    st.error("Rate Limit Exceeded (HTTP 429)!")
                    st.json(resp.json())
                else:
                    st.warning(f"Response Status: {resp.status_code}")
                    st.write(resp.text)

                with st.expander("Inspected Response Headers"):
                    st.json(dict(resp.headers))

            except Exception as e:
                st.error(f"Failed to connect to EdgeFlow at {url}: {e}")
                st.info("Make sure EdgeFlow is running: `uvicorn edgeflow.main:app --port 8000`")

# --- TAB 2: Failover & Resilience ---
with tab2:
    st.subheader("Simulated Primary Outage & Zero-Downtime Failover")
    st.markdown("""
    When your primary LLM/service (e.g. OpenAI) suffers downtime or 5xx errors,
    EdgeFlow's Circuit Breaker trips and **instantly reroutes traffic to the standby provider** (e.g. Claude) with zero caller disruption.
    """)

    st.code("""
    Primary Pool:   [ OpenAI Target 1, OpenAI Target 2 ]   --> 💥 Down / 503 / Timeout
                            │
                            ▼ (Auto Failover)
    Fallback Pool:  [ Anthropic Claude Standby ]          --> ✅ Active (X-EdgeFlow-Fallback: true)
    """, language="text")

    st.info("To test failover, start the mock upstream servers: `python mock_services/upstream_server.py --port 9001`")

# --- TAB 3: Routes & Circuits ---
with tab3:
    st.subheader("Active Routing Table & Circuit Breaker Health")
    admin_headers = dict(headers)
    admin_headers["X-API-Key"] = "ef-live-admin-key"

    if st.button("Refresh Gateway State 🔄"):
        try:
            r_routes = httpx.get(f"{gateway_url}/api/v1/admin/routes", headers=admin_headers, timeout=3.0)
            r_circuits = httpx.get(f"{gateway_url}/api/v1/admin/circuits", headers=admin_headers, timeout=3.0)

            if r_routes.status_code == 200:
                st.write("### Active Routes")
                st.json(r_routes.json())
            if r_circuits.status_code == 200:
                st.write("### Target Circuit Breakers")
                st.json(r_circuits.json())
        except Exception as e:
            st.warning(f"Gateway offline or not responding: {e}")

# --- TAB 4: Prometheus Telemetry ---
with tab4:
    st.subheader("Prometheus Metrics Stream")
    if st.button("Fetch Raw /metrics 📊"):
        try:
            m_resp = httpx.get(f"{gateway_url}/metrics", timeout=3.0)
            st.text(m_resp.text)
        except Exception as e:
            st.warning(f"Could not connect to /metrics: {e}")

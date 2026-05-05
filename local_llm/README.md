# Local LLM Infrastructure

Standalone local inference stack using **OpenVINO Model Server (OVMS)** + **LiteLLM proxy**. Exposes a single `http://localhost:4000` endpoint that speaks the Anthropic Messages API, so any project using the Anthropic Python SDK can point here instead of the cloud.

This folder contains only infrastructure — no application code lives here.

---

## Prerequisites

- **OVMS binary** installed at `~/.local/opt/ovms/bin/ovms`
  - Install guide: https://docs.openvino.ai/2024/ovms_docs_deploying_server.html
- **WSL2** with an Intel GPU and `/usr/lib/wsl/lib` present (for GPU acceleration)
- **Python 3.13** and **uv** installed
- Run `uv sync` inside this directory once to create the local venv:
  ```bash
  cd local_llm/
  uv sync
  ```

---

## Downloading the Model

The model weights are not stored in this repo (7.1 GB binary). Download via `huggingface-cli`:

```bash
pip install huggingface_hub
# Replace <hf-model-id> with the exact HuggingFace repo you downloaded from
# (e.g. OpenVINO/Qwen2.5-7B-Instruct-int8-ov or similar)
huggingface-cli download <hf-model-id> \
    --local-dir models/qwen2.5-7b-openvino \
    --include "1/openvino_model.bin" "tokenizer.json" "vocab.json" "merges.txt"
```

The tokenizer OpenVINO graphs (`openvino_tokenizer.*`, `openvino_detokenizer.*`) and all
config files are already in the repo — only the weight binary needs downloading.

---

## Starting the Stack

Run these three steps in order.

### 1. Start OVMS

```bash
./ovms.sh start gpu
```

OVMS loads the model weights (~1–3 min on first run). The script polls until the model is ready:

```
ovms-gpu is AVAILABLE — REST http://localhost:8000  gRPC localhost:9000
```

### 2. Start LiteLLM proxy

In a separate terminal:

```bash
uv run python config.py
```

LiteLLM starts on port 4000 and routes requests to OVMS.

### 3. Smoke test

```bash
uv run python test_litellm.py
```

Expected: a short response from the model printed to stdout.

---

## Stopping the Stack

```bash
# Stop LiteLLM: Ctrl+C in its terminal

# Stop OVMS:
./ovms.sh stop gpu

# Verify nothing is running:
./ovms.sh status
```

---

## Connecting From a Project

Point the Anthropic SDK at `localhost:4000` with the master key from `litellm_config.yaml`:

```python
import anthropic

client = anthropic.Anthropic(
    base_url="http://localhost:4000",
    api_key="sk-local-key",
)

message = client.messages.create(
    model="qwen-openvino",   # see model table below
    max_tokens=512,
    messages=[{"role": "user", "content": "Hello"}],
)
print(message.content[0].text)
```

### Available Models

| model_name      | backend                        | requires OVMS? |
|-----------------|--------------------------------|----------------|
| `qwen-openvino` | OVMS on localhost:8000         | yes            |
| `qwen-ollama`   | Ollama at 192.168.118.30:11434 | no             |

No other code changes are needed — the rest of the Anthropic SDK API works identically.

---

## Adding a New Model

### Step 1 — Add the model files

Place the model under `models/your-model-name/`. It must follow the OVMS MediaPipe layout:

```
models/your-model-name/
├── graph.pbtxt          # MediaPipe pipeline config — use /model as path placeholder
├── ovms_config.json     # registers the MediaPipe graph with OVMS
├── generation_config.json
├── tokenizer.json / tokenizer_config.json / ...
└── 1/
    ├── openvino_model.xml
    ├── openvino_model.bin
    ├── openvino_tokenizer.xml / .bin
    └── openvino_detokenizer.xml / .bin
```

Use `models/qwen2.5-7b-openvino/` as a reference. The key rule: `graph.pbtxt` must use `/model` as the path placeholder — `ovms.sh` patches it to the real absolute path at runtime.

### Step 2 — Update `ovms.sh`

Duplicate the `prepare_configs()` block or extend it to patch your new model's configs. The patching pattern is the same `sed` substitution: `/model` → absolute path.

For multi-model serving (both models loaded simultaneously), add a second entry to `ovms_config.json`'s `mediapipe_config_list` and patch both `graph.pbtxt` files in `prepare_configs()`.

### Step 3 — Add an entry to `litellm_config.yaml`

```yaml
model_list:
  # ... existing entries ...
  - model_name: your-model-alias        # name clients use
    litellm_params:
      model: openai/your-ovms-model-name  # must match name in ovms_config.json
      api_base: http://localhost:8000/v3
      api_key: sk-na
```

### Step 4 — Restart LiteLLM

```bash
# Ctrl+C the running config.py, then:
uv run python config.py
```

OVMS restart is only needed if the new model was not loaded in the current OVMS session.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| OVMS fails to start | Check binary exists: `ls ~/.local/opt/ovms/bin/ovms` |
| Model never reaches AVAILABLE | Check log: `tail -f /tmp/ovms_runtime/ovms-gpu.log` — look for `state changed to: AVAILABLE` |
| LiteLLM returns 502 | Ensure OVMS is running and model is AVAILABLE before starting LiteLLM |
| Port 4000 already in use | Edit `config.py` to pass a different port, e.g. `start_proxy(port=4001)` |
| GPU device not found | Verify Intel GPU driver: `./ovms.sh status` shows available OpenVINO devices |
| `clBuildProgram` errors in log | GPU driver version mismatch — try `start cpu` variant by editing `graph.pbtxt`: change `device: "GPU"` → `device: "CPU"` |

Log files:
- OVMS: `/tmp/ovms_runtime/ovms-gpu.log`
- LiteLLM: stdout of `config.py`
